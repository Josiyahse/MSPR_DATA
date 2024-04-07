import json
import os
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Text
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Chemin du fichier JSON
current_directory = os.path.dirname(os.path.abspath(__file__))
JSON_FILE = os.path.join(current_directory, 'candidates.json')

# Base de données
DB_CONNECTION = Variable.get("AIRFLOW_DB_CONNECTION")
Base = declarative_base()


# Modèles
class PoliticalParty(Base):
  __tablename__ = 'political_parties'
  id = Column(Integer, primary_key=True)
  name = Column(String(255), nullable=False)


class Candidate(Base):
  __tablename__ = 'candidates'
  id = Column(Integer, primary_key=True)
  gender = Column(String(1))
  firstname = Column(String(255))
  lastname = Column(String(255))
  birthday = Column(Date)
  party_id = Column(Integer, ForeignKey('political_parties.id'))
  party = relationship(PoliticalParty)


class Proposition(Base):
  __tablename__ = 'propositions'
  id = Column(Integer, primary_key=True)
  year = Column(Integer)
  declarations = Column(Text)
  candidate_id = Column(Integer, ForeignKey('candidates.id'))
  candidate = relationship(Candidate)


# Fonctions
def create_tables(engine):
  Base.metadata.create_all(engine)


def load_json():
  with open(JSON_FILE, 'r') as file:
    return json.load(file)


def save_to_postgres():
  engine = create_engine(DB_CONNECTION)
  create_tables(engine)
  Session = sessionmaker(bind=engine)
  session = Session()
  data = load_json()

  for candidate_data in data:
    # Vérifier si le candidat existe déjà
    existing_candidate = session.query(Candidate).filter_by(
      firstname=candidate_data['firstname'],
      lastname=candidate_data['lastname'],
      birthday=datetime.strptime(candidate_data['birthday'], '%Y-%m-%d').date()
    ).first()

    # Si le candidat n'existe pas, le créer
    if not existing_candidate:
      party = session.query(PoliticalParty).filter_by(name=candidate_data['parti']).first()
      if not party:
        party = PoliticalParty(name=candidate_data['parti'])
        session.add(party)
        session.commit()

      candidate = Candidate(
        gender=candidate_data['gender'],
        firstname=candidate_data['firstname'],
        lastname=candidate_data['lastname'],
        birthday=datetime.strptime(candidate_data['birthday'], '%Y-%m-%d').date(),
        party_id=party.id
      )
      session.add(candidate)
      session.commit()
    else:
      candidate = existing_candidate

    # Vérifier si la proposition pour l'année et le candidat existe déjà
    existing_proposition = session.query(Proposition).filter_by(
      year=2017,
      candidate_id=candidate.id
    ).first()

    # Si la proposition n'existe pas pour cette année et ce candidat, la créer
    if not existing_proposition:
      propositions = "; ".join(candidate_data['propositions'])
      proposition = Proposition(
        year=2017,
        declarations=propositions,
        candidate_id=candidate.id
      )
      session.add(proposition)
  with sessionmaker(bind=engine)() as session:
    # Your logic here
    session.commit()


# DAG
default_args = {
  "owner": "airflow",
  "depends_on_past": False,
  "start_date": datetime(2024, 3, 26),
  "email_on_failure": False,
  "email_on_retry": False,
}

dag = DAG(
  "load_candidates_2017",
  default_args=default_args,
  description="Load candidates from JSON to PostgreSQL",
  tags=["candidates"]
)

save_to_db_task = PythonOperator(
  task_id="save_to_postgres_candidates",
  python_callable=save_to_postgres,
  dag=dag,
)

save_to_db_task
