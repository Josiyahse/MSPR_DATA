from airflow.models import Variable
from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.sql import text

# Configuration
DEPARTMENT_ID = Variable.get("DEPARTMENT_ID")  # ID du département à fixer en constante
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")

# Configurez le logger
logger = logging.getLogger("airflow.task")

# Assurez-vous de configurer les connexions à votre environnement Airflow avant d'exécuter ce DAG
engine = create_engine(DB_CONNECTION)


# Fonctions Python pour exécuter les tâches
def aggregate_data():
  # Connexion à la base de données
  with engine.connect() as conn:
    query = text("""
            SELECT
    e.candidate_id,
    d.name AS department_name,
    e.round,
    e.votes AS candidate_votes,
    i.rate_per_thousand AS insecurity_rate,
    u.total AS unemployment_rate,
    pr.intentions AS vote_intentions,
    c.firstname || ' ' || c.lastname AS candidate_name,
    pp.name AS party_name,
    STRING_AGG(DISTINCT pos.name, ', ') AS party_positions,
    prop.declarations AS candidate_propositions,
    vi.subscribes AS number_of_registered,
    vi.voters AS total_votes,
    vi.nulls AS null_votes,
    vi.express AS expressed_votes
FROM election_results e
JOIN insecurities i ON e.department_id = i.department_id
JOIN unemployment u ON e.department_id = u.department_id
JOIN poll_rounds pr ON e.candidate_id = pr.candidate_id AND e.year = pr.year
JOIN candidates c ON e.candidate_id = c.id
JOIN departments d ON e.department_id = d.id
JOIN political_parties pp ON c.party_id = pp.id
LEFT JOIN party_positions ppn ON pp.id = ppn.party_id
LEFT JOIN positions pos ON ppn.position_id = pos.id
JOIN propositions prop ON c.id = prop.candidate_id AND e.year = prop.year
JOIN vote_information vi ON e.department_id = vi.department_id AND e.year = vi.year
WHERE e.department_id = :department_id AND e.round = 2
GROUP BY e.candidate_id, d.name, e.round, e.votes, i.rate_per_thousand, u.total, pr.intentions, c.firstname, c.lastname, pp.name, prop.declarations, vi.subscribes, vi.voters, vi.nulls, vi.express
""")
    result = conn.execute(query, {'department_id': DEPARTMENT_ID}).fetchall()
    df = pd.DataFrame(result, columns=result[0].keys())

    #df = df.drop_duplicates(subset=['insecurities'])

    # Création de la table si elle n'existe pas
    if not inspect(engine).has_table("election_data_set_round_2"):
      df.to_sql('election_data_set', con=engine, index=False)
      logger.info("Table 'election_data_set' created and data inserted successfully.")
    else:
      df.to_sql('election_data_set', con=engine, if_exists='replace', index=False)
      logger.info("Table 'election_data_set' exists. Data inserted successfully.")


# Définition du DAG
default_args = {
  "owner": "airflow",
  "depends_on_past": False,
  "start_date": datetime(2024, 3, 26),
  "email_on_failure": False,
  "email_on_retry": False,
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
