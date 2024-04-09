from airflow.models import Variable
from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine, inspect

# Configuration
URL = "https://www.data.gouv.fr/fr/datasets/r/2776519f-a940-46f0-99f4-1a3a1374193b"  # Remplacez ceci par l'URL de votre fichier CSV
FILENAME = "Presidentielle_2017_Resultats_Tour_1_c.xls"
SHEET_NAME = "Départements Tour 1"
HEADER = 3  # Les index commencent à 0, donc 3 signifie la quatrième ligne
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")

# Configurez le logger
logger = logging.getLogger("airflow.task")

year_of_election = 2017


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
  engine = create_engine(DB_CONNECTION)
  # Lire le fichier Excel, en traitant initialement tous les codes comme des chaînes
  # Charger la feuille spécifique du fichier XLS
  df = pd.read_excel(FILENAME, sheet_name=SHEET_NAME, header=HEADER)

  df['Code du département'] = df['Code du département'].astype(str)

  # df = pd.read_excel(URL, header=[0])
  # Mapping des noms de colonnes pour les départements
  department_columns = {
    "Code du département": "code_department",
  }
  df.rename(columns=department_columns, inplace=True)
  df['code_department'] = df['code_department'].apply(lambda x: x if len(x) > 2 else x.zfill(2))

  df['code_department'] = df['code_department'].apply(convert_department_code)

  # Traitement des données des départements
  departments_df = pd.read_sql("SELECT id, code_department FROM departments", con=engine)
  departments_df['code_department'] = departments_df['code_department'].astype(int)

  df = df.merge(departments_df, how='left', left_on="code_department", right_on="code_department",
                suffixes=('', '_dept'))

  # Vérifiez que la colonne de l'ID du département est correctement nommée 'id' après la fusion
  if 'id_dept' not in df.columns and 'id' in df.columns:
    df.rename(columns={'id': 'department_id'}, inplace=True)

  # Lecture des données des candidats
  candidates_df = pd.read_sql("SELECT id, lastname FROM candidates", con=engine)

  # Initialiser une liste pour stocker les données réorganisées
  new_data = []

  # Assurez-vous que la variable year_of_election est définie quelque part dans votre code
  # year_of_election = 2024

  for row_index, row in df.iterrows():
    # Parcourir chaque cellule de la ligne à la recherche de correspondances de candidats
    for candidate_index, candidate_lastname in candidates_df['lastname'].items():
      # Trouver l'index de la cellule qui contient le nom du candidat
      candidate_positions = row[row == candidate_lastname].index.tolist()
      if candidate_positions:
        for start_position in candidate_positions:
          # Assurez-vous que start_position est un entier pour l'utiliser avec iloc
          if isinstance(start_position, str) and start_position.startswith("Unnamed"):
            start_position = int(start_position.replace("Unnamed: ", ""))
          else:
            # Si c'est le nom de la colonne, obtenir l'index numérique correspondant
            start_position = df.columns.get_loc(start_position)

          votes = row.iloc[start_position + 2]
          vote_per_subscribe = row.iloc[start_position + 3]
          vote_per_express = row.iloc[start_position + 4]

          candidate_data = {
            'department_id': row['department_id'],
            'candidate_id': candidates_df.loc[candidates_df['lastname'] == candidate_lastname, 'id'].values[0],
            'votes': votes,
            'vote_per_subscribe': vote_per_subscribe,
            'vote_per_express': vote_per_express,
            'round': 1,  # Assurez-vous que la valeur du round est correcte
            'year': year_of_election
          }
          new_data.append(candidate_data)

  # Convertir la liste en un nouveau DataFrame
  df_candidates = pd.DataFrame(new_data)

  # Filtrer le DataFrame pour ne garder que les colonnes nécessaires
  df_candidates = df_candidates[
    ['department_id', 'candidate_id', 'votes', 'vote_per_subscribe', 'vote_per_express', 'round', 'year']]

  # Retirer les lignes où candidate_id ou department_id est NA
  df_candidates.dropna(subset=['candidate_id', 'department_id'], inplace=True)

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
    # logger.info(df_to_save)
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

dag = DAG("election_data_processing_round_1_2017", default_args=default_args, tags=["2017_election_result"])

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
