from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable
import numpy as np
import logging

# Configurez le logger
logger = logging.getLogger("airflow.task")
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2024, 1, 1),
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
}

dag = DAG(
  'bootstrap_future_elections',
  default_args=default_args,
  description='Generate future election data using bootstrapping',
  schedule_interval=timedelta(days=1),
)


def extract_data(**kwargs):
  ti = kwargs['ti']
  engine = create_engine(DB_CONNECTION)
  df = pd.read_sql("SELECT * FROM election_data_set", con=engine)
  ti.xcom_push(key='historical_election_data', value=df.to_json(orient='split'))


def bootstrap_data(**kwargs):
  ti = kwargs['ti']
  historical_data_json = ti.xcom_pull(task_ids='extract_data', key='historical_election_data')
  historical_data = pd.read_json(historical_data_json, orient='split')

  # Créez un DataFrame vide pour stocker les données futures
  future_data_frames = []

  # Générer des données futures pour les prochaines élections
  for year in range(2027, 2038, 5):  # 2027, 2032, 2037 sont les 3 prochaines élections après 2022
    sampled = historical_data.sample(n=len(historical_data), replace=True)
    sampled['year'] = year
    future_data_frames.append(sampled)

  # Concaténer toutes les DataFrames
  future_data = pd.concat(future_data_frames, ignore_index=True)

  ti.xcom_push(key='bootstrapped_data', value=future_data.to_json(orient='split'))


def save_future_data(**kwargs):
  ti = kwargs['ti']
  bootstrapped_data_json = ti.xcom_pull(task_ids='bootstrap_data', key='bootstrapped_data')
  bootstrapped_data = pd.read_json(bootstrapped_data_json, orient='split')

  engine = create_engine(DB_CONNECTION)
  bootstrapped_data.to_sql('future_election_result', con=engine, if_exists='replace', index=False)


# Task definitions
extract_data_task = PythonOperator(
  task_id='extract_data',
  python_callable=extract_data,
  dag=dag,
)

bootstrap_data_task = PythonOperator(
  task_id='bootstrap_data',
  python_callable=bootstrap_data,
  dag=dag,
)

save_future_data_task = PythonOperator(
  task_id='save_future_data',
  python_callable=save_future_data,
  dag=dag,
)

# Task ordering
extract_data_task >> bootstrap_data_task >> save_future_data_task
