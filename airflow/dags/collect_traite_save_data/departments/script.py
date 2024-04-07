from airflow.models import Variable
from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text



# Configuration
URL = "https://www.data.gouv.fr/fr/datasets/r/70cef74f-70b1-495a-8500-c089229c0254"  # Remplacez ceci par l'URL de votre fichier CSV
FILENAME = "departements-france.csv"
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")
# REGION_CODE = 11

# Configurez le logger
logger = logging.getLogger("airflow.task")


# Télécharger le fichier CSV
def download_csv_file():
  response = requests.get(URL)
  with open(FILENAME, "wb") as file:
    file.write(response.content)


def convert_department_code(code):
  if code == "2A":
    return 265  # Code ASCII de 'A' est 65
  elif code == "2B":
    return 266  # Code ASCII de 'B' est 66
  if code == "ZA":
    return 971
  elif code == "ZB":
    return 972
  if code == "ZC":
    return 973
  elif code == "ZD":
    return 974
  if code == "ZM":
    return 976
  try:
    return int(code)
  except ValueError:
    return code

# Nettoyer et transformer les données
def clean_and_transform_data():
  df = pd.read_csv(FILENAME)

  # Convertir les codes des départements
  df['code_departement'] = df['code_departement'].apply(convert_department_code)

  # Aucune filtration par région n'est nécessaire
  return df


# Enregistrer dans PostgreSQL
def save_to_postgres():
  if DB_CONNECTION is None:
    logger.error("La variable d'environnement DB_CONNECTION n'est pas définie.")
  else:
    logger.info(f"La chaîne de connexion est: {DB_CONNECTION}")
    try:
      engine = create_engine(DB_CONNECTION)
      # Créer la table si elle n'existe pas
      inspector = inspect(engine)
      if not inspector.has_table("departments"):
        create_departments_sql = """
                CREATE TABLE departments (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    code_department INTEGER UNIQUE,
                    code_region INTEGER
                );
                """
        engine.execute(create_departments_sql)
        logger.info("Table 'departments' created successfully.")

      # Nettoyer et transformer les données
      df_to_save = clean_and_transform_data()
      df_to_save.rename(columns={'nom_departement': 'name',
                                 'code_departement': 'code_department',
                                 'code_region': 'code_region'}, inplace=True)
      df_to_save = df_to_save[['name', 'code_department', 'code_region']]

      # Parcourir chaque ligne du DataFrame pour vérifier et insérer si nécessaire
      # Dans votre boucle d'insertion :
      for _, row in df_to_save.iterrows():
        exist_query = text("SELECT EXISTS (SELECT 1 FROM departments WHERE code_department = :code_department)")
        exists = engine.execute(exist_query, {'code_department': row['code_department']}).scalar()

        if not exists:
          insert_query = text("""
                  INSERT INTO departments (name, code_department, code_region)
                  VALUES (:name, :code_department, :code_region)
              """)
          engine.execute(insert_query, {
            'name': row['name'],
            'code_department': row['code_department'],
            'code_region': row['code_region']
          })
          logger.info(f"Inserted department with code {row['code_department']}")
        else:
          logger.info(f"Department with code {row['code_department']} already exists. Skipping.")

    except Exception as e:
      logger.error(f"Une erreur s'est produite lors de la vérification ou de l'insertion des données : {e}")
      raise


# Définition du DAG
default_args = {
  "owner": "airflow",
  "depends_on_past": False,
  "start_date": datetime(2024, 3, 26),
  "email_on_failure": False,
  "email_on_retry": False,
}

dag = DAG("data_processing_pipeline_departments", default_args=default_args)

t1 = PythonOperator(task_id="download_csv_file", python_callable=download_csv_file, dag=dag)
t3 = PythonOperator(task_id="clean_and_transform_data", python_callable=clean_and_transform_data, dag=dag)
t4 = PythonOperator(task_id="save_to_postgres", python_callable=save_to_postgres, dag=dag)

t1 >> t3 >> t4
