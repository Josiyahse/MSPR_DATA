import pandas as pd
import os
import logging
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
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
  df = pd.read_sql("SELECT * FROM election_data_set WHERE round = 1", con=engine)

  df = df.drop(columns=['candidate_id','year','candidate_name',])

  # Sélectionnez vos caractéristiques X
  X_features = ['department_name', 'average_insecurity_rate', 'unemployment_rate', 'number_of_registered',
                'total_votes', 'expressed_votes', 'party_name', 'party_positions',
                'candidate_propositions', 'vote_intentions', 'round',
                'null_votes']  # Assurez-vous que ces colonnes existent
  X = df[X_features]

  # Il est conseillé de gérer les données catégorielles, comme 'department_name' et 'party_name'
  X = pd.get_dummies(X, columns=['department_name', 'party_name', 'party_positions', 'candidate_propositions'])

  # La cible Y est maintenant 'candidate_votes', qui est une valeur continue
  y = df['candidate_votes']

  # Divisez les données en ensemble d'apprentissage et de test
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

  # Store the processed data and target variables for train in XComs
  ti.xcom_push(key='X_train', value=X_train.to_json(orient='split'))
  ti.xcom_push(key='X_test', value=X_test.to_json(orient='split'))
  ti.xcom_push(key='y_train', value=y_train.to_json(orient='records', lines=True))
  ti.xcom_push(key='y_test', value=y_test.to_json(orient='index'))


def train_decision_tree(**kwargs):
  ti = kwargs['ti']
  X_train = pd.read_json(ti.xcom_pull(key='X_train', task_ids='preprocess_data'), orient='split')
  y_train = pd.read_json(ti.xcom_pull(key='y_train', task_ids='preprocess_data'), orient='records', lines=True)

  # Utiliser un modèle de régression
  clf = DecisionTreeRegressor()
  clf.fit(X_train, y_train)

  # Sauvegarder le modèle dans un fichier
  model_filename = f"decision_tree_regressor_model_election_prediction.pkl"
  model_filepath = os.path.join('/opt/airflow/models', model_filename)
  model_column_filename = f"model_columns_election_prediction.pkl"
  model_column_filepath = os.path.join('/opt/airflow/models', model_column_filename)

  logger.info(f"Saving model to {model_filepath}")
  with open(model_filepath, 'wb') as file:
    pickle.dump(clf, file)

  # Après l'entraînement du modèle
  model_columns = list(X_train.columns)
  with open(model_column_filepath, 'wb') as file:
    pickle.dump(model_columns, file)

  ti.xcom_push(key='model_path', value=model_filepath)


def evaluate_model(**kwargs):
  ti = kwargs['ti']
  model_path = ti.xcom_pull(key='model_path', task_ids='train_decision_tree')

  # Load the model from the file
  with open(model_path, 'rb') as file:
    clf = pickle.load(file)

  # Pull the test set
  X_test_json = ti.xcom_pull(key='X_test', task_ids='preprocess_data')
  y_test_json = ti.xcom_pull(key='y_test', task_ids='preprocess_data')

  # Convert JSON to DataFrame
  X_test = pd.read_json(X_test_json, orient='split')
  y_test = pd.read_json(y_test_json, orient='index')

  # Make predictions
  y_pred = clf.predict(X_test)

  # Calculez les métriques de régression
  mse = mean_squared_error(y_test, y_pred)
  rmse = sqrt(mse)  # Racine carrée du MSE pour avoir le RMSE
  mae = mean_absolute_error(y_test, y_pred)
  r2 = r2_score(y_test, y_pred)  # Coefficient de détermination

  # Affichez les métriques
  print(f'MSE = {mse}')
  print(f'RMSE = {rmse}')
  print(f'MAE = {mae}')
  print(f'R-squared = {r2}')

  # Push the metrics to XComs
  ti.xcom_push(key='evaluation', value={
    'mse': mse,
    'r2': r2,
    'rmse': rmse,
    'mae': mae,
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


preprocess_data_task = PythonOperator(
  task_id='preprocess_data',
  python_callable=preprocess_data,
  provide_context=True,
  dag=dag,
)

train_model_task = PythonOperator(
  task_id='train_decision_tree',
  python_callable=train_decision_tree,
  provide_context=True,
  dag=dag,
)

evaluate_model_task = PythonOperator(
  task_id='evaluate_model',
  python_callable=evaluate_model,
  provide_context=True,
  dag=dag,
)

# Ensuite, vous configurez vos tâches PythonOperator pour sauvegarder les résultats :
save_results_task = PythonOperator(
  task_id='save_results',
  python_callable=save_results,
  provide_context=True,
  dag=dag,
)

preprocess_data_task >> train_model_task >> evaluate_model_task >> save_results_task
