from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import pickle
from airflow.models import Variable

DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")
MODEL_PATH = '/opt/airflow/models/decision_tree_regressor_model_20240406.pkl'
MODEL_COLUMN_PATH = '/opt/airflow/models/model_columns_20240406.pkl'

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
  'decision_tree_prediction_pipeline',
  default_args=default_args,
  description='Predict winners for the next three years',
  schedule_interval='@yearly',
)


def predict_winners(**kwargs):
  engine = create_engine(DB_CONNECTION)
  with open(MODEL_PATH, 'rb') as model_file:
    model = pickle.load(model_file)

  with open(MODEL_COLUMN_PATH, 'rb') as file:
    model_columns = pickle.load(file)

  # Votre requête SQL devra être ajustée pour refléter correctement la façon dont vos données sont stockées et les années que vous souhaitez prédire.
  query = """
      SELECT *
      FROM election_data_set
      WHERE year >= 2022 AND round = 2
      """
  future_data = pd.read_sql_query(query, con=engine)

  # Supprimez l'utilisation de '...' et spécifiez les colonnes réelles à supprimer
  columns_to_drop = ['candidate_id', 'candidate_votes', 'null_votes', 'expressed_votes']  # ajoutez plus si nécessaire
  X_predict = future_data.drop(columns=columns_to_drop)

  # Aligner les colonnes avec celles du modèle
  X_predict = X_predict.reindex(columns=model_columns, fill_value=0)

  # Faire les prédictions avec le modèle
  predictions = model.predict(X_predict)

  # Construire le DataFrame de prédictions
  predictions_df = pd.DataFrame(predictions, columns=['predicted_votes'])
  predictions_df['year'] = future_data['year']
  predictions_df['department'] = future_data['department_name']
  predictions_df['party_name'] = future_data['party_name']  # Ajoutez le nom du parti

  # Supposons que vous voulez trouver les deux partis avec le plus de voix prédites pour le second tour
  predictions_df['rank'] = predictions_df.groupby(['year', 'department'])['predicted_votes'].rank(method='first',
                                                                                                  ascending=False)
  winners_df = predictions_df[predictions_df['rank'] <= 2]

  # Sauvegarder les prédictions dans la base de données
  winners_df.to_sql('winners_predictions', con=engine, if_exists='replace', index=False)

  print("Predictions saved to the database.")


predict_winners_task = PythonOperator(
  task_id='predict_winners',
  python_callable=predict_winners,
  provide_context=True,
  dag=dag,
)

predict_winners_task
