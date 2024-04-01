from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine, inspect

# Configuration
URL = "https://www.data.gouv.fr/fr/datasets/r/e7b263e5-bae2-43cc-8944-c8daae6f7ff6"
FILENAME = "resultats-par-niveau-dpt-t2-france-entiere.xlsx"  # Mettez à jour le chemin vers le fichier téléchargé
DB_CONNECTION = "postgresql+psycopg2://airflow:airflow@172.16.5.3:5432/postgres"

# Configurez le logger
logger = logging.getLogger("airflow.task")

year_of_election = 2022


def download_xlsx_file():
  response = requests.get(URL)
  with open(FILENAME, "wb") as file:
    file.write(response.content)


def clean_and_transform_data():
  engine = create_engine(DB_CONNECTION)
  df = pd.read_excel(FILENAME, dtype={'Code du département': str})

  # Mapping des noms de colonnes pour les départements
  department_columns = {
    "Code du département": "code_department",
  }
  df.rename(columns=department_columns, inplace=True)

  df['code_department'] = df['code_department'].apply(lambda x: x if len(x) > 2 else x.zfill(2))

  # Traitement des données des départements
  departments_df = pd.read_sql("SELECT id, code_department FROM departments", con=engine)
  departments_df['code_department'] = departments_df['code_department'].astype(str)
  df = df.merge(departments_df, how='left', left_on="code_department", right_on="code_department",
                suffixes=('', '_dept'))

  # Vérifiez que la colonne de l'ID du département est correctement nommée 'id' après la fusion
  if 'id_dept' not in df.columns and 'id' in df.columns:
    df.rename(columns={'id': 'department_id'}, inplace=True)

  # Lecture des données des candidats
  candidates_df = pd.read_sql("SELECT id, lastname FROM candidates", con=engine)

  # Assumer que chaque groupe de 6 colonnes correspond à un candidat
  num_candidate_info_cols = 6
  # Initialiser une liste pour stocker les données réorganisées
  new_data = []

  for row_index, row in df.iterrows():
    for i in range(0, len(row) - num_candidate_info_cols, num_candidate_info_cols):
      # Extrait les données pour le candidat actuel
      candidate_data = {
        'department_id': row['department_id'],
        'candidate_lastname': row[i],  # i est l'index où le nom du candidat apparaît
        'votes': row[i + 2],
        'vote_per_subscribe': row[i + 3],
        'vote_per_express': row[i + 4],
        'round': 2,
        'year': year_of_election
      }
      new_data.append(candidate_data)

  # Convertir la liste en un nouveau DataFrame
  df_candidates = pd.DataFrame(new_data)

  # Associer un candidate_id à chaque entrée en fonction du lastname
  df_candidates['candidate_id'] = df_candidates['candidate_lastname'].map(
    candidates_df.set_index('lastname')['id']
  ).astype(pd.Int64Dtype())

  # Filtrer le DataFrame pour ne garder que les colonnes nécessaires
  df_candidates = df_candidates[
    ['department_id', 'candidate_id', 'votes', 'vote_per_subscribe', 'vote_per_express', 'round', 'year']]

  # Retirer les lignes où candidate_id est NA
  df_candidates.dropna(subset=['candidate_id'], inplace=True)

  # Retirer les lignes où department_id est NA
  df_candidates.dropna(subset=['department_id'], inplace=True)

  return df_candidates


def save_to_postgres():
  try:
    engine = create_engine(DB_CONNECTION)
    inspector = inspect(engine)
    if not inspector.has_table("election_results"):
      create_table_sql = """
                CREATE TABLE election_results (
                    id SERIAL PRIMARY KEY,
                    department_id INTEGER,
                    candidate_id INTEGER,
                    votes INTEGER,
                    vote_per_subscribe FLOAT,
                    vote_per_express FLOAT,
                    round INTEGER,
                    year INTEGER,
                    UNIQUE(department_id, candidate_id, round, year),
                    FOREIGN KEY (department_id) REFERENCES departments(id),
                    FOREIGN KEY (candidate_id) REFERENCES candidates(id)
                )
            """
      engine.execute(create_table_sql)
      logger.info("Table 'election_results' created successfully.")

    df_to_save = clean_and_transform_data()
    df_to_save.to_sql(name="election_results", con=engine, if_exists="append", index=False)
    logger.info("Data saved to PostgreSQL successfully.")
  except Exception as e:
    logger.error(f"Error: {e}")
    raise


# Définition du DAG
default_args = {
  "owner": "airflow",
  "depends_on_past": False,
  "start_date": datetime(2024, 3, 26),
  "email_on_failure": False,
  "email_on_retry": False,
}

dag = DAG("election_data_processing_round_2", default_args=default_args, schedule_interval="@daily")

t1 = PythonOperator(
  task_id="download_xlsx_file",
  python_callable=download_xlsx_file,
  dag=dag)
t2 = PythonOperator(
  task_id="clean_and_transform_data",
  python_callable=clean_and_transform_data,
  dag=dag)
t3 = PythonOperator(
  task_id="save_to_postgres",
  python_callable=save_to_postgres,
  dag=dag)

t1 >> t2 >> t3
