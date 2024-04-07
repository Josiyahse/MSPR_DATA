import logging
from unidecode import unidecode  # Pour normaliser les accents
from datetime import datetime, timedelta
import requests
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Configuration de la base de données
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")
Base = declarative_base()

# Configuration du logger
logger = logging.getLogger("airflow.task")


# Définition des modèles
class Candidate(Base):
  __tablename__ = 'candidates'
  id = Column(Integer, primary_key=True)
  lastname = Column(String(255), nullable=False)
  firstname = Column(String(255), nullable=False)
  poll_rounds = relationship('PollRound', back_populates='candidate')


class PollRound(Base):
  __tablename__ = 'poll_rounds'
  id = Column(Integer, primary_key=True)
  candidate_id = Column(Integer, ForeignKey('candidates.id'))
  intentions = Column(Float)
  erreur_sup = Column(Float)
  erreur_inf = Column(Float)
  round = Column(Integer)
  year = Column(Integer)
  candidate = relationship('Candidate', back_populates='poll_rounds')


# Fonctions pour le traitement des données
def create_tables(engine):
  Base.metadata.create_all(engine)


def fetch_poll_data():
  response = requests.get('https://raw.githubusercontent.com/nsppolls/nsppolls/master/presidentielle.json')
  response.raise_for_status()
  return response.json()


def process_and_save_poll_data():
  engine = create_engine(DB_CONNECTION)
  create_tables(engine)
  Session = sessionmaker(bind=engine)
  session = Session()

  def normalize_name(name):
    normalized = unidecode(name.lower())
    logger.info(f"Normalizing name: {name} to {normalized}")
    return normalized

  data = fetch_poll_data()
  candidate_polls = {}

  for survey in data:
    survey_year = datetime.strptime(survey['fin_enquete'], '%Y-%m-%d').year
    for round_info in survey.get('tours', []):
      round_number = 1 if round_info['tour'] == "Premier tour" else 2
      for hypothesis in round_info.get('hypotheses', []):
        for candidate_info in hypothesis.get('candidats', []):
          candidate_name = candidate_info.get('candidat')
          if candidate_name:
            candidate_name_parts = candidate_info['candidat'].split(' ')
            first_name_part = normalize_name(candidate_name_parts[0])
            last_name_part = normalize_name(candidate_name_parts[-1])

            candidate = session.query(Candidate) \
              .filter(Candidate.firstname.ilike(f"%{first_name_part}%"),
                      Candidate.lastname.ilike(f"%{last_name_part}%")) \
              .first()
            logger.info(f"{candidate},")
            if candidate:
              candidate_key = (candidate.id, survey_year, round_number)
              polls = candidate_polls.setdefault(candidate_key, {'intentions': [], 'erreur_sup': [], 'erreur_inf': []})
              polls['intentions'].append(candidate_info['intentions'])
              polls['erreur_sup'].append(candidate_info['erreur_sup'])
              polls['erreur_inf'].append(candidate_info['erreur_inf'])

  # logger.info(candidate_polls)
  # Calcul et sauvegarde des moyennes
  for candidate_key, poll_data in candidate_polls.items():
    candidate_id, year, round_number = candidate_key
    avg_data = {k: sum(v) / len(v) for k, v in poll_data.items()}

    existing_poll_round = session.query(PollRound).filter(and_(
      PollRound.candidate_id == candidate_id,
      PollRound.year == year,
      PollRound.round == round_number
    )).first()

    if existing_poll_round:
      existing_poll_round.intentions, existing_poll_round.erreur_sup, existing_poll_round.erreur_inf = avg_data.values()
    else:
      session.add(PollRound(candidate_id=candidate_id, year=year, round=round_number, **avg_data))

  session.commit()


# Configuration du DAG Airflow
default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2024, 3, 26),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'email_on_failure': False,
  'email_on_retry': False,
  'max_active_runs': 1
}

dag = DAG(
  'load_poll_data',
  default_args=default_args,
  description='Load poll data from JSON to PostgreSQL',
)

t1 = PythonOperator(
  task_id='process_and_save_poll_data',
  python_callable=process_and_save_poll_data,
  dag=dag,
)

t1
