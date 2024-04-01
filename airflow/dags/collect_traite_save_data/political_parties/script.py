import json
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Charger les variables d'environnement depuis .env
load_dotenv()

DB_CONNECTION = "postgresql+psycopg2://airflow:airflow@172.16.5.3:5432/postgres"

current_directory = os.path.dirname(os.path.abspath(__file__))
JSON_FILE = os.path.join(current_directory, 'parties.json')

Base = declarative_base()


class PoliticalParty(Base):
  __tablename__ = 'political_parties'

  id = Column(Integer, primary_key=True)
  name = Column(String(255), nullable=False)
  creation_date = Column(Integer)
  positions = relationship('PartyPosition', back_populates='party')


class Position(Base):
  __tablename__ = 'positions'

  id = Column(Integer, primary_key=True)
  name = Column(String(255), nullable=False, unique=True)


class PartyPosition(Base):
  __tablename__ = 'party_positions'

  party_id = Column(Integer, ForeignKey('political_parties.id'), primary_key=True)
  position_id = Column(Integer, ForeignKey('positions.id'), primary_key=True)
  party = relationship(PoliticalParty, back_populates='positions')
  position = relationship(Position)


def create_tables(engine):
  Base.metadata.create_all(engine)


def load_json():
  with open(JSON_FILE, 'r') as file:
    data = json.load(file)
  return data


def save_to_postgres():
  engine = create_engine(DB_CONNECTION)
  create_tables(engine)

  Session = sessionmaker(bind=engine)
  session = Session()

  data = load_json()

  for party_data in data:
    party = session.query(PoliticalParty).filter_by(name=party_data['name']).first()
    if not party:
      party = PoliticalParty(name=party_data['name'], creation_date=int(party_data['birthday']))
      session.add(party)
    for position_name in party_data['position']:
      position = session.query(Position).filter_by(name=position_name).first()
      if not position:
        position = Position(name=position_name)
        session.add(position)
      party_position = PartyPosition(party=party, position=position)
      session.add(party_position)
  session.commit()


default_args = {
  "owner": "airflow",
  "depends_on_past": False,
  "start_date": datetime(2024, 3, 26),
  "email_on_failure": False,
  "email_on_retry": False,
}

dag = DAG(
  "load_political_parties",
  default_args=default_args,
  description="Load political parties from JSON to PostgreSQL",
  schedule_interval="@daily",
)

save_to_db_task = PythonOperator(
  task_id="save_to_postgres_parties",
  python_callable=save_to_postgres,
  dag=dag,
)

save_to_db_task
