from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import zipfile
import os
from dbfread import DBF
import pandas as pd
from sqlalchemy import create_engine

# Configuration
URL = "https://www.insee.fr/fr/statistiques/fichier/3555153/fd_eec17_dbase.zip"
FILENAME = "fd_eec17_dbase.zip"
EXTRACT_PATH = "airflow/dags/dowloads"
DB_CONNECTION = "postgresql+psycopg2://airflow:airflow@172.18.0.2:5432/postgres"

# Télécharger le fichier ZIP
def download_zip_file():
    response = requests.get(URL)
    with open(FILENAME, "wb") as file:
        file.write(response.content)

# Dézipper le fichier
def unzip_file():
    with zipfile.ZipFile(FILENAME, "r") as zip_ref:
        zip_ref.extractall(EXTRACT_PATH)

# Nettoyer et transformer les données
def clean_and_transform_data():
    data_frames = []  # Créer une liste vide pour stocker les dataframes
    for filename in os.listdir(EXTRACT_PATH):
        if filename.endswith(".dbf"):
            try:
                # Spécifier l'encodage lors de la lecture du fichier DBF
                table = DBF(os.path.join(EXTRACT_PATH, filename), load=True, encoding='latin1')
                df = pd.DataFrame(iter(table))
                # Ici, vous pouvez ajouter vos opérations de nettoyage des données
                data_frames.append(df)  # Ajouter le dataframe à la liste
            except UnicodeDecodeError as e:
                print(f"Erreur de décodage Unicode lors de la lecture du fichier {filename}: {e}")
                # Gérer l'erreur ou passer au fichier suivant
    return data_frames  # Retourner la liste complète des dataframes

# Enregistrer dans PostgreSQL
def save_to_postgres():
    engine = create_engine(DB_CONNECTION)
    for df in clean_and_transform_data():
        df.to_sql(name="chaumage", con=engine, if_exists="append", index=False)

# Définition du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
}

dag = DAG("data_processing_pipeline_chaumage_2017", default_args=default_args , schedule_interval="@daily")

t1 = PythonOperator(task_id="download_zip_file", python_callable=download_zip_file, dag=dag)
t2 = PythonOperator(task_id="unzip_file", python_callable=unzip_file, dag=dag)
t3 = PythonOperator(task_id="clean_and_transform_data", python_callable=clean_and_transform_data, dag=dag)
t4 = PythonOperator(task_id="save_to_postgres", python_callable=save_to_postgres, dag=dag)

t1 >> t2 >> t3 >> t4
