from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score
from sqlalchemy import create_engine
import pandas as pd
import pickle

# Configuration
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")
MODEL_PATH = "/path/to/your/saved/model.pkl"  # Remplacez par votre chemin de fichier

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
  'model_training_pipeline',
  default_args=default_args,
  description='A simple model training pipeline',
  schedule_interval=timedelta(days=1),
)


def train_model():
  # Création d'un moteur de connexion à la base de données
  engine = create_engine(DB_CONNECTION)

  # Chargement des données
  df = pd.read_sql("SELECT * FROM election_data_set", con=engine)

  # Séparez les caractéristiques et la cible
  X = df.drop('vote_intentions', axis=1)  # ou toute autre colonne cible
  y = df['vote_intentions']  # la variable à prédire

  # Séparation en jeux d'entraînement et de test
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

  # Prétraitement pour les colonnes numériques et catégorielles
  numeric_features = ['insecurity_rate', 'unemployment_rate', 'expressed_votes']  # etc.
  numeric_transformer = StandardScaler()

  categorical_features = ['department_name', 'party_name', 'party_positions']  # etc.
  categorical_transformer = OneHotEncoder(handle_unknown='ignore')

  # Création d'un transformateur de colonnes
  preprocessor = ColumnTransformer(
    transformers=[
      ('num', numeric_transformer, numeric_features),
      ('cat', categorical_transformer, categorical_features)
    ])

  # Création du pipeline avec prétraitement et modèle
  model = Pipeline(steps=[('preprocessor', preprocessor),
                          ('classifier', RandomForestClassifier())])

  # Entraînement du modèle
  model.fit(X_train, y_train)

  # Sauvegarde du modèle
  with open(MODEL_PATH, 'wb') as model_file:
    pickle.dump(model, model_file)

  # Prédiction sur le jeu de test
  y_pred = model.predict(X_test)

  # Évaluation du modèle
  accuracy = accuracy_score(y_test, y_pred)
  print(f"Model Accuracy: {accuracy}")


train_model_task = PythonOperator(
  task_id='train_model',
  python_callable=train_model,
  dag=dag,
)

train_model_task
