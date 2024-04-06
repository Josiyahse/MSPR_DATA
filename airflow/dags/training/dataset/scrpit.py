from airflow.models import Variable
from airflow import DAG
import json
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.sql import text

# Configuration
REGION_CODE = 11
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")
ROUNDS = json.loads(Variable.get("ROUNDS"))

# Assurez-vous que ROUNDS est une liste d'entiers
ROUNDS = [int(round) for round in ROUNDS]

# Configurez le logger
logger = logging.getLogger("airflow.task")

# Assurez-vous de configurer les connexions à votre environnement Airflow avant d'exécuter ce DAG
engine = create_engine(DB_CONNECTION)


# Fonctions Python pour exécuter les tâches
def aggregate_data():
  # Connexion à la base de données
  with engine.connect() as conn:
    # Initialiser le DataFrame final pour tous les tours
    df_all_rounds = pd.DataFrame()
    for round_number in [1, 2]:  # Parcourir les deux tours
      query = text("""
      WITH CandidatePositions AS (
    SELECT
        c.id AS candidate_id,
        STRING_AGG(DISTINCT pos.name, ', ') AS party_positions
    FROM candidates c
    JOIN political_parties pp ON c.party_id = pp.id
    LEFT JOIN party_positions ppn ON pp.id = ppn.party_id
    LEFT JOIN positions pos ON ppn.position_id = pos.id
    GROUP BY c.id
),
AggregatedInsecurity AS (
    SELECT
        department_id,
        AVG(rate_per_thousand) AS average_insecurity_rate
    FROM insecurities
    GROUP BY department_id
),
VoteInformationFiltered AS (
    SELECT
        department_id,
        round,
        SUM(subscribes) AS total_subscribes,
        SUM(voters) AS total_voters,
        SUM(nulls) AS total_nulls,
        SUM(express) AS total_express
    FROM vote_information
    GROUP BY department_id, round
)
SELECT
    e.candidate_id,
    d.name AS department_name,
    e.year,
    e.round,
    e.votes AS candidate_votes,
    ai.average_insecurity_rate,
    u.total AS unemployment_rate,
    pr.intentions AS vote_intentions,
    c.firstname || ' ' || c.lastname AS candidate_name,
    pp.name AS party_name,
    cp.party_positions,
    prop.declarations AS candidate_propositions,
    vif.total_subscribes AS number_of_registered,
    vif.total_voters AS total_votes,
    vif.total_nulls AS null_votes,
    vif.total_express AS expressed_votes
FROM election_results e
JOIN departments d ON e.department_id = d.id
JOIN candidates c ON e.candidate_id = c.id
JOIN political_parties pp ON c.party_id = pp.id
JOIN AggregatedInsecurity ai ON e.department_id = ai.department_id
JOIN unemployment u ON e.department_id = u.department_id
JOIN poll_rounds pr ON e.candidate_id = pr.candidate_id AND e.round = pr.round AND e.year = pr.year
JOIN propositions prop ON e.candidate_id = prop.candidate_id AND e.year = prop.year
JOIN VoteInformationFiltered vif ON e.department_id = vif.department_id AND e.round = vif.round
JOIN CandidatePositions cp ON e.candidate_id = cp.candidate_id
WHERE d.code_region = :region_code -- ou un autre paramètre si nécessaire
AND e.round = :round_number -- utilisez la variable de liaison pour le numéro de tour
ORDER BY e.candidate_id, e.round, d.name;
      """)

      # Exécuter la requête pour le tour actuel
      result = conn.execute(query, {'region_code': REGION_CODE, 'round_number': round_number}).fetchall()
      df_round = pd.DataFrame(result, columns=result[0].keys())

      # Concaténer les résultats du tour avec le DataFrame final
      df_all_rounds = pd.concat([df_all_rounds, df_round], ignore_index=True)

    # Supprimer les doublons si nécessaire
    df_all_rounds.drop_duplicates(inplace=True)

    # Création de la table si elle n'existe pas ou insertion des données
    df_all_rounds.to_sql('election_data_set', con=engine, if_exists='replace', index=False)
    logger.info("Table 'election_data_set' updated with round data.")


# Reste du code DAG...


# Définition du DAG
default_args = {
  "owner": "airflow",
  "depends_on_past": False,
  "start_date": datetime(2024, 3, 26),
  "email_on_failure": False,
  "email_on_retry": False,
  'max_active_runs': 1
}

dag = DAG(
  "data_processing_pipeline_election_data_set",
  default_args=default_args,
  schedule_interval="@daily"
)

aggregate_task = PythonOperator(
  task_id="aggregate_data",
  python_callable=aggregate_data,
  dag=dag
)

aggregate_task
