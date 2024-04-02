from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable

# Configuration
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")  # Assurez-vous d'avoir défini cette Variable Airflow
MODEL_TABLE = "election_predictions"  # Nom de la table pour stocker les prédictions


def preprocess_data():
  # Connectez-vous à la base de données et chargez les données nécessaires
  engine = create_engine(DB_CONNECTION)
  df = pd.read_sql("SELECT * FROM election_results", engine)
  # Effectuez le prétraitement des données ici (nettoyage, encodage, etc.)
  # Sauvegardez les données prétraitées pour les utiliser dans l'étape suivante
  df.to_csv('/tmp/preprocessed_data.csv', index=False)


def train_and_evaluate_model():
  # Chargez les données prétraitées
  df = pd.read_csv('/tmp/preprocessed_data.csv')
  # Séparez les données en fonctionnalités et cible
  X = df.drop('target_column', axis=1)
  y = df['target_column']
  # Séparez les données en ensembles d'entraînement et de test
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
  # Entraînez le modèle
  model = RandomForestClassifier(n_estimators=100, random_state=42)
  model.fit(X_train, y_train)
  # Évaluez le modèle
  accuracy = accuracy_score(y_test, model.predict(X_test))
  print(f'Model Accuracy: {accuracy:.2f}')
  # Sauvegardez le modèle entraîné pour les prédictions futures


def make_predictions():
  # Chargez le modèle entraîné
  # Faites des prédictions sur les données futures
  # Stockez les prédictions dans la base de données PostgreSQL pour visualisation dans Redash
  engine = create_engine(DB_CONNECTION)
  # Supposons que vous ayez une fonction qui charge votre modèle et fait des prédictions
  # predictions = your_model_predict_function(future_data)
  # df_predictions = pd.DataFrame(predictions, columns=['predictions'])
  # df_predictions.to_sql(MODEL_TABLE, engine, if_exists='replace', index=False)


# Définition du DAG
dag = DAG(
  'election_modeling_pipeline',
  default_args={
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 26),
  },
  description='A DAG for Election Predictive Modeling',
  schedule_interval=None,  # Assumez qu'il s'agit d'un DAG exécuté manuellement
)

t0 = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data, dag=dag)
t1 = PythonOperator(task_id='train_and_evaluate_model', python_callable=train_and_evaluate_model, dag=dag)
t2 = PythonOperator(task_id='make_predictions', python_callable=make_predictions, dag=dag)

t0 >> t1 >> t2
