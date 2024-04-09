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
URL = "https://www.data.gouv.fr/fr/datasets/r/e7b263e5-bae2-43cc-8944-c8daae6f7ff6"
FILENAME = "resultats-par-niveau-dpt-t2-france-entiere.xls"  # Mettez à jour le chemin vers le fichier téléchargé
SHEET_NAME = "Résultats par niveau Dpt T2 Fra"
HEADER = 0  # Les index commencent à 0, donc 3 signifie la quatrième ligne
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")

# Configurez le logger
logger = logging.getLogger("airflow.task")

# Définir la constante pour l'année de vote
YEAR_OF_VOTE = 2017


# Télécharger le fichier xls
def download_xlsx_file():
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


def clean_and_transform_data():
  # Supposons que DB_CONNECTION et FILENAME sont définis précédemment
  engine = create_engine(DB_CONNECTION)
  # Lire le fichier Excel, en traitant initialement tous les codes comme des chaînes
  df = pd.read_excel(FILENAME, dtype={'Code du département': str})

  df['Code du département'] = df['Code du département'].astype(str)

  # Appliquer la fonction de conversion au code du département si nécessaire
  # Assurez-vous que cette fonction existe et convertit les codes de département comme prévu
  df['Code du département'] = df['Code du département'].apply(lambda x: x if len(x) > 2 else x.zfill(2))

  # Convertir les codes des départements
  df['Code du département'] = df['Code du département'].apply(convert_department_code)

  # Lecture et traitement des données des départements depuis la base de données
  departments_df = pd.read_sql("SELECT id, code_department FROM departments", con=engine)
  departments_df['code_department'] = departments_df['code_department'].astype(int)

  # Mappage des noms de colonnes pour df
  column_mapping = {
    "Inscrits": "subscribes",
    "Abstentions": "abstentions",
    "Votants": "voters",
    "Blancs": "whites",
    "Nuls": "nulls",
    "Exprimés": "express",
    "% Abs/Ins": "abstention_per_subscribe",
    "% Vot/Ins": "voter_per_subscribe",
    "% Blancs/Ins": "white_per_subscribe",
    "% Blancs/Vot": "white_per_voter",
    "% Nuls/Vot": "null_per_voter",
    "% Nuls/Ins": "null_per_subscribe",
    "% Exp/Ins": "express_per_subscribe",
    "% Exp/Vot": "express_per_voter",
    "Code du département": "code_department"  # Utiliser temporairement 'code_department'
  }
  df.rename(columns=column_mapping, inplace=True)

  # Fusion basée sur la colonne de code de département prétraitée
  # Utiliser suffixes pour distinguer les colonnes avec des noms identiques dans les deux DataFrames
  df_merged = df.merge(departments_df, how='inner', left_on="code_department", right_on="code_department",
                       suffixes=('', '_dept'))

  # Ajuster les colonnes à conserver après la fusion pour inclure l'`id` du département
  columns_to_keep = ['id', 'subscribes', 'abstentions', 'voters', 'whites', 'nulls', 'express',
                     'abstention_per_subscribe', 'voter_per_subscribe', 'white_per_subscribe', 'white_per_voter',
                     'null_per_voter', 'null_per_subscribe', 'express_per_subscribe', 'express_per_voter']
  df_final = df_merged[columns_to_keep]

  # Renommer 'id' en 'department_id' pour correspondre à vos attentes
  df_final.rename(columns={'id': 'department_id'}, inplace=True)

  # Vous pouvez ici sauvegarder df_final dans un fichier ou une base de données selon le besoin
  # Exemple : df_final.to_excel('chemin_vers_le_fichier.xlsx', index=False)

  # Ajouter la colonne year avec la valeur constante
  df_final['year'] = YEAR_OF_VOTE
  df_final['round'] = 2

  return df_final


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
      if not inspector.has_table("vote_information"):
        create_table_sql = """
                    CREATE TABLE vote_information (
                        id SERIAL PRIMARY KEY,
                        subscribes INTEGER,
                        abstentions INTEGER,
                        voters INTEGER,
                        whites  INTEGER,
                        nulls  INTEGER,
                        express  INTEGER,
                        abstention_per_subscribe FLOAT,
                        voter_per_subscribe FLOAT,
                        white_per_subscribe FLOAT,
                        white_per_voter FLOAT,
                        null_per_voter FLOAT,
                        null_per_subscribe FLOAT,
                        express_per_subscribe FLOAT,
                        express_per_voter FLOAT,
                        year INT,
                        round INT,
                        department_id INTEGER,
                        CONSTRAINT fk_department
                        FOREIGN KEY (department_id)
                        REFERENCES departments (id)
                    )
                    """
        engine.execute(create_table_sql)
        logger.info("Table 'vote_information' created successfully.")

      df_to_save = clean_and_transform_data()
      df_to_save.to_sql(name="vote_information", con=engine, if_exists="append", index=False)
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

dag = DAG("data_processing_pipeline_vote_information_round_2_2017", default_args=default_args,
          tags=["2017_vote_information"])

t1 = PythonOperator(task_id="download_xlsx_file", python_callable=download_xlsx_file, dag=dag)
t2 = PythonOperator(task_id="clean_and_transform_data", python_callable=clean_and_transform_data, dag=dag)
t3 = PythonOperator(task_id="save_to_postgres", python_callable=save_to_postgres, dag=dag)

t1 >> t2 >> t3
