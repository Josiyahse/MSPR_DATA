from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import zipfile
import os
from io import BytesIO
import logging

default_args = {
  'owner': 'vous',
  'depends_on_past': False,
  'start_date': datetime(2024, 3, 23),
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
}

dag = DAG(
  'pipeline_traitement_donnees',
  default_args=default_args,
  description='Un pipeline pour télécharger, nettoyer, traiter des données et les exporter',
  schedule_interval=None,
)


def telecharger_fichier():
  url = "https://www.insee.fr/fr/statistiques/fichier/3555153/fd_eec17_dbase.zip"
  response = requests.get(url)
  if response.status_code == 200:
    z = zipfile.ZipFile(BytesIO(response.content))
    z.extractall(path="/path/to/destination")
    logging.info("Fichier téléchargé et extrait.")
  else:
    logging.error("Le fichier n'a pas pu être téléchargé.")


tache_telecharger = PythonOperator(
  task_id='telecharger_fichier',
  python_callable=telecharger_fichier,
  dag=dag,
)


def traiter_fichier():
  dossier = "/path/to/destination"
  for filename in os.listdir(dossier):
    if filename.endswith(".csv"):  # Vous ajustez selon le type de fichier que vous attendez
      chemin_complet = os.path.join(dossier, filename)
      df = pd.read_csv(chemin_complet)
      logging.info(df.head(10))
    else:
      logging.error("Type de fichier non reconnu.")


tache_traiter = PythonOperator(
  task_id='traiter_fichier',
  python_callable=traiter_fichier,
  dag=dag,
)

tache_telecharger >> tache_traiter
