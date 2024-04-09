import pandas as pd
import os
import logging
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from math import sqrt
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import pickle
import uuid

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
  'decision_tree_training_pipeline',
  default_args=default_args,
  description='Train a decision tree model',
  schedule_interval=timedelta(days=1),
)


def preprocess_data(**kwargs):
  ti = kwargs['ti']
  engine = create_engine(DB_CONNECTION)
  df = pd.read_sql("SELECT * FROM election_data_set", con=engine)

  # Sélectionnez vos caractéristiques X et la colonne cible y
  X = df.drop(columns=['candidate_id', 'candidate_votes', 'candidate_name', 'department_name'])

  # La colonne cible 'y' est 'candidate_votes'
  y = df['candidate_votes']

  # Prétraitement des caractéristiques catégorielles avec one-hot encoding
  X = pd.get_dummies(X, columns=['party_name', 'party_positions', 'candidate_propositions'])

  # Diviser les données en ensembles d'apprentissage et de test
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

  # Stocker les données traitées pour un usage ultérieur dans le modèle
  ti.xcom_push(key='X_train', value=X_train.to_json(orient='split'))
  ti.xcom_push(key='X_test', value=X_test.to_json(orient='split'))
  ti.xcom_push(key='y_train', value=y_train.tolist())  # Changement ici pour enregistrer y_train en tant que liste
  ti.xcom_push(key='y_test', value=y_test.tolist())  # Changement ici pour enregistrer y_test en tant que liste


def train_regression_model(**kwargs):
  ti = kwargs['ti']
  X_train_json = ti.xcom_pull(task_ids='preprocess_data', key='X_train')
  y_train_list = ti.xcom_pull(task_ids='preprocess_data', key='y_train')

  X_train = pd.read_json(X_train_json, orient='split')
  y_train = pd.Series(y_train_list)  # Chargement de y_train en tant que pd.Series

  reg = DecisionTreeRegressor()
  reg.fit(X_train, y_train)

  # Sauvegarde du modèle entraîné dans un fichier
  model_filename = "decision_tree_regressor_model_election_prediction.pkl"
  model_filepath = os.path.join('/opt/airflow/models', model_filename)

  logger.info(f"Saving model to {model_filepath}")
  with open(model_filepath, 'wb') as file:
    pickle.dump(reg, file)

  model_columns = list(X_train.columns)
  model_column_filename = "model_columns_election_prediction.pkl"
  model_column_filepath = os.path.join('/opt/airflow/models', model_column_filename)

  with open(model_column_filepath, 'wb') as file:
    pickle.dump(model_columns, file)

  ti.xcom_push(key='model_path', value=model_filepath)


def evaluate_model(**kwargs):
  ti = kwargs['ti']
  model_path = ti.xcom_pull(task_ids='train_regression_model', key='model_path')
  X_test_json = ti.xcom_pull(task_ids='preprocess_data', key='X_test')
  y_test_list = ti.xcom_pull(task_ids='preprocess_data', key='y_test')

  X_test = pd.read_json(X_test_json, orient='split')
  y_test = pd.Series(y_test_list)  # Convertissez la liste en pd.Series directement

  with open(model_path, 'rb') as file:
    reg = pickle.load(file)

  y_pred = reg.predict(X_test)

  mse = mean_squared_error(y_test, y_pred)
  mae = mean_absolute_error(y_test, y_pred)
  r2 = r2_score(y_test, y_pred)

  print(f'MSE: {mse}')
  print(f'MAE: {mae}')
  print(f'R^2: {r2}')

  ti.xcom_push(key='evaluation', value={
    'mse': mse,
    'mae': mae,
    'r2': r2
  })


def save_results(**kwargs):
  ti = kwargs['ti']
  logger.info("Saving evaluation results")

  # Récupérer les résultats d'évaluation directement
  evaluation_results = ti.xcom_pull(key='evaluation', task_ids='evaluate_model')

  # Vérifiez si evaluation_results n'est pas None
  if evaluation_results is None:
    raise ValueError("No evaluation results found")

  # Convertir les résultats en DataFrame pandas
  results_df = pd.DataFrame([evaluation_results])

  # Enregistrer les résultats dans la base de données
  engine = create_engine(DB_CONNECTION)
  results_df.to_sql('evaluation_results', con=engine, if_exists='append', index=False)

  logger.info("Results successfully saved to the database")


# Configuration des tâches PythonOperator
preprocess_data_task = PythonOperator(
  task_id='preprocess_data',
  python_callable=preprocess_data,
  provide_context=True,
  dag=dag,
)

train_model_task = PythonOperator(
  task_id='train_regression_model',
  python_callable=train_regression_model,
  provide_context=True,
  dag=dag,
)

evaluate_model_task = PythonOperator(
  task_id='evaluate_model',
  python_callable=evaluate_model,
  provide_context=True,
  dag=dag,
)

save_results_task = PythonOperator(
  task_id='save_results',
  python_callable=save_results,
  provide_context=True,
  dag=dag,
)

preprocess_data_task >> train_model_task >> evaluate_model_task >> save_results_task
