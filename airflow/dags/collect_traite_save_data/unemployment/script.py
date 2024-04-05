from airflow.models import Variable
from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect


# Configuration
URL = "https://www.insee.fr/fr/statistiques/fichier/2012804/sl_etc_2023T4.xls"  # Remplacez ceci par l'URL de votre fichier CSV
FILENAME = "sl_etc_2023T4.xls"
SHEET_NAME = "Département"
HEADER = 3  # Les index commencent à 0, donc 3 signifie la quatrième ligne
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")

# Configurez le logger
logger = logging.getLogger("airflow.task")


# Télécharger le fichier xls
def download_xls_file():
  response = requests.get(URL)
  with open(FILENAME, "wb") as file:
    file.write(response.content)

def convert_department_code(code):
  if code == "2A":
    return 265  # Code ASCII de 'A' est 65
  elif code == "2B":
    return 266  # Code ASCII de 'B' est 66
  try:
    return int(code)
  except ValueError:
    return code

# Nettoyer et transformer les données
def clean_and_transform_data():
  # Charger la feuille spécifique du fichier XLS
  df = pd.read_excel(FILENAME, sheet_name=SHEET_NAME, header=HEADER)

  # Connexion à la base de données pour accéder à la table 'departments'
  engine = create_engine(DB_CONNECTION)
  df_departments = pd.read_sql("SELECT id, code_department FROM departments", engine)

  # Convertir les codes des départements
  df['Code'] = df['Code'].apply(convert_department_code)

  # Remplacer le code du département par l'ID correspondant
  df = df.merge(df_departments, left_on='Code', right_on='code_department', how='left')

  # Générer dynamiquement les noms des colonnes de trimestre pour l'année 2022
  trimester_columns_2022 = [f'T{i}_2022' for i in range(1, 5)]

  # Vérifier l'existence des colonnes dans le DataFrame et les conserver si présentes
  trimester_columns_2022 = [col for col in trimester_columns_2022 if col in df.columns]

  # Attribuer l'année à toute la DataFrame
  df['year'] = 2022  # Définit directement l'année puisque vous travaillez avec 2022

  # Calculer le total comme la moyenne des trimestres pour l'année 2022
  df['total'] = df[trimester_columns_2022].mean(axis=1)

  # Préparer le DataFrame pour l'insertion
  df_to_save = df.rename(columns={'id': 'department_id'}).copy()

  # Exclure les lignes où 'department_id' est NaN
  df_to_save = df_to_save.dropna(subset=['department_id'])

  # Convertir 'department_id' en entier
  df_to_save['department_id'] = df_to_save['department_id'].astype(int)

  # Assurez-vous que les colonnes trimestrielles correctes et autres nécessaires sont incluses
  df_to_save['year'] = 2022
  df_to_save['total'] = df_to_save[trimester_columns_2022].mean(axis=1)

  # Préparer df_to_save pour inclure les bonnes colonnes
  df_to_save = df_to_save[['year', 'total', 'department_id'] + trimester_columns_2022]

  return df_to_save


# Enregistrer dans PostgreSQL
def save_to_postgres():
  if DB_CONNECTION is None:
    logger.error("La variable d'environnement DB_CONNECTION n'est pas définie.")
  else:
    logger.info(f"La chaîne de connexion est: {DB_CONNECTION}")
    # Essayez de créer l'objet engine maintenant
    try:
      engine = create_engine(DB_CONNECTION)
      # Créer la table si elle n'existe pas
      inspector = inspect(engine)
      if not inspector.has_table("unemployment"):
        create_table_sql = """
                    CREATE TABLE unemployment (
                        id SERIAL PRIMARY KEY,
                        year INTEGER,
                        total FLOAT,
                        trimester_1 FLOAT,
                        trimester_2  FLOAT,
                        trimester_3  FLOAT,
                        trimester_4  FLOAT,
                        department_id INTEGER,
                        CONSTRAINT fk_department
                        FOREIGN KEY (department_id)
                        REFERENCES departments (id)
                    )
                    """
        engine.execute(create_table_sql)
        logger.info("Table 'unemployment' created successfully.")

      # Enregistrer les données
      df_to_save = clean_and_transform_data()
      # Renommer les colonnes pour qu'elles correspondent à la table
      df_to_save.rename(columns={
        'year': 'year',
        'T1_2022': 'trimester_1',
        'T2_2022': 'trimester_2',
        'T3_2022': 'trimester_3',
        'T4_2022': 'trimester_4',
        'total': 'total',
        'department_id': 'department_id'
      }, inplace=True)
      # S'assurer que le DataFrame contient uniquement les colonnes de la table
      df_to_save = df_to_save[[
        'year',
        'trimester_1',
        'trimester_2',
        'trimester_3',
        'trimester_4',
        'total',
        'department_id',
      ]]
      # Enregistrer dans la base de données
      df_to_save.to_sql(name="unemployment", con=engine, if_exists="append", index=False)
    except Exception as e:
      logger.error(f"Une erreur s'est produite lors de la création de l'engine ou de l'insertion des données : {e}")
      raise


# Définition du DAG
default_args = {
  "owner": "airflow",
  "depends_on_past": False,
  "start_date": datetime(2024, 3, 26),
  "email_on_failure": False,
  "email_on_retry": False,
}

dag = DAG("data_processing_pipeline_unemployment", default_args=default_args)

t1 = PythonOperator(task_id="download_xls_file", python_callable=download_xls_file, dag=dag)
t2 = PythonOperator(task_id="clean_and_transform_data", python_callable=clean_and_transform_data, dag=dag)
t3 = PythonOperator(task_id="save_to_postgres", python_callable=save_to_postgres, dag=dag)

t1 >> t2 >> t3
