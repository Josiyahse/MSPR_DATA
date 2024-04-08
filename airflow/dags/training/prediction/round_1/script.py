from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import pickle
from airflow.models import Variable

DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")
MODEL_PATH = '/opt/airflow/models/decision_tree_regressor_model_election_prediction.pkl'
MODEL_COLUMN_PATH = '/opt/airflow/models/model_columns_election_prediction.pkl'

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
  'decision_tree_prediction_pipeline-round_1',
  default_args=default_args,
  description='Predict winners for the next three years',
  schedule_interval='@yearly',
)


def predict_winners(**kwargs):
  engine = create_engine(DB_CONNECTION)
  with open(MODEL_PATH, 'rb') as model_file:
    reg_model = pickle.load(model_file)  # Modèle de régression

  with open(MODEL_COLUMN_PATH, 'rb') as file:
    model_columns = pickle.load(file)

  # On sélectionne uniquement les données pour les années futures prédéfinies
  for year in range(2027, 2038, 5):  # 2027, 2032, 2037
    query = f"""
          SELECT *
          FROM future_election_result
          WHERE round = 1 AND year = {year}
      """
    future_data = pd.read_sql_query(query, con=engine)

    # Préparation des données pour la prédiction
    X_predict = future_data.drop(columns=['candidate_id', 'year', 'candidate_name', 'candidate_votes'])
    X_predict = pd.get_dummies(X_predict,
                               columns=['department_name', 'party_name', 'party_positions', 'candidate_propositions'])
    X_predict = X_predict.reindex(columns=model_columns, fill_value=0)

    # Prédiction du nombre de voix pour chaque candidat dans chaque département
    future_data['predicted_votes'] = reg_model.predict(X_predict)

    # Sauvegarder les prédictions dans une nouvelle table par année
    future_data['year'] = year  # Ajoutez l'année à la table de prédiction
    future_data.to_sql(f'predicted_votes_{year}_round_1', con=engine, if_exists='replace', index=False)

    # Agrégation des voix prédites par candidat sur tous les départements par année
    total_predicted_votes_by_candidate = future_data.groupby(['year', 'candidate_name'])[
      'predicted_votes'].sum().reset_index()

    # Sauvegarder les résultats agrégés dans une nouvelle table
    total_predicted_votes_by_candidate.to_sql('predicted_votes_by_candidate_and_year_round_1', con=engine, if_exists='append',
                                              index=False)

    print(f"Predicted votes for {year} saved to the database.")


predict_winners_task = PythonOperator(
  task_id='predict_winners_round_1',
  python_callable=predict_winners,
  provide_context=True,
  dag=dag,
)

predict_winners_task
