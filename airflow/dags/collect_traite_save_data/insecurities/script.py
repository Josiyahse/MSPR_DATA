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
URL = "https://www.data.gouv.fr/fr/datasets/r/acc332f6-92be-42af-9721-f3609bea8cfc"  # Remplacez ceci par l'URL de votre fichier CSV
FILENAME = "donnee-dep-data.gouv-2023-geographie2023-produit-le2024-03-07.csv"
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")

# Configurez le logger
logger = logging.getLogger("airflow.task")


# Télécharger le fichier CSV
def download_csv_file():
  response = requests.get(URL)
  with open(FILENAME, "wb") as file:
    file.write(response.content)


# Nettoyer et transformer les données
def clean_and_transform_data():
  df = pd.read_csv(FILENAME, delimiter=';')
  engine = create_engine(DB_CONNECTION)
  logger.info(df.columns.tolist())

  # Attempt to convert 'Code.département' to integers, setting errors to NaN
  df['Code.département'] = pd.to_numeric(df['Code.département'], errors='coerce')

  # Drop rows where 'Code.département' could not be converted to an integer
  df = df.dropna(subset=['Code.département'])

  # Convert 'Code.département' to integer after dropping NaN values
  df['Code.département'] = df['Code.département'].astype(int)

  # Similarly, convert the 'code_department' in the 'departments' DataFrame
  df_departments = pd.read_sql("SELECT id, code_department FROM departments", engine)
  df_departments['code_department'] = pd.to_numeric(df_departments['code_department'], errors='coerce').astype(int)

  # Merge including the 'id' from 'departments' based on the 'Code.département'
  df_merged = df.merge(df_departments, left_on='Code.département', right_on='code_department', how='left')

  # Set the year value to "22" for all rows
  df_merged['annee'] = '22'

  # Convert the year to a complete string format
  df_merged['annee'] = '20' + df_merged['annee'].astype(str).str.zfill(2)

  # Convert 'LOG' column to integers to keep only the integer part
  df_merged['LOG'] = df_merged['LOG'].str.replace(',', '.').astype(float).astype(int)

  # Remplacer les virgules par des points et convertir en flottant
  df_merged['tauxpourmille'] = df_merged['tauxpourmille'].str.replace(',', '.').astype(float)

  # Rename columns as needed and include the 'department_id'
  df_merged.rename(columns={
    'id': 'department_id',  # This will be the foreign key
    'annee': 'year',  # Rename and format the year column
    # Add any other columns that need renaming or formatting here
  }, inplace=True)

  # Suppression des lignes où 'department_id' est NaN
  df_merged = df_merged.dropna(subset=['department_id'])

  # Drop the 'Code.département' column as it's replaced by 'department_id'
  df_final = df_merged.drop(columns=['Code.département', 'code_department'])

  # Ensure you select only the columns you want to insert into 'insecurities' table
  df_final = df_final[[
    # List all the columns from the CSV that you want to keep here
    'classe',
    'year',
    'faits',
    'unité.de.compte',
    'POP',
    'LOG',
    'millPOP',
    'millLOG',
    'tauxpourmille',
    'department_id'
  ]]

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
      if not inspector.has_table("insecurities"):
        create_table_sql = """
                    CREATE TABLE insecurities (
                        id SERIAL PRIMARY KEY,
                        classe VARCHAR(255),
                        year INTEGER,
                        facts INTEGER,
                        compt_unit VARCHAR(255),
                        population INTEGER,
                        habitation FLOAT,
                        population_per_thousand INTEGER,
                        habitation_per_thousand INTEGER,
                        rate_per_thousand FLOAT,
                        department_id INTEGER,
                        CONSTRAINT fk_department
                        FOREIGN KEY (department_id)
                        REFERENCES departments (id)
                    )
                    """
        engine.execute(create_table_sql)
        logger.info("Table 'insecurities' created successfully.")

      # Enregistrer les données
      df_to_save = clean_and_transform_data()
      # Renommer les colonnes pour qu'elles correspondent à la table
      df_to_save.rename(columns={'classe': 'classe',
                                 'year': 'year',
                                 'faits': 'facts',
                                 'unité.de.compte': 'compt_unit',
                                 'POP': 'population',
                                 'LOG': 'habitation',
                                 'millPOP': 'population_per_thousand',
                                 'millLOG': 'habitation_per_thousand',
                                 'tauxpourmille': 'rate_per_thousand',
                                 'Code_department': 'departement_id'
                                 }, inplace=True)
      # S'assurer que le DataFrame contient uniquement les colonnes de la table
      df_to_save = df_to_save[['classe',
                               'year',
                               'facts',
                               'compt_unit',
                               'population',
                               'habitation',
                               'population_per_thousand',
                               'habitation_per_thousand',
                               'rate_per_thousand',
                               'department_id']]
      # Enregistrer dans la base de données
      df_to_save.to_sql(name="insecurities", con=engine, if_exists="append", index=False)
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

dag = DAG("data_processing_pipeline_insecurities", default_args=default_args)

t1 = PythonOperator(task_id="download_csv_file", python_callable=download_csv_file, dag=dag)
t2 = PythonOperator(task_id="clean_and_transform_data", python_callable=clean_and_transform_data, dag=dag)
t3 = PythonOperator(task_id="save_to_postgres", python_callable=save_to_postgres, dag=dag)

t1 >> t2 >> t3
