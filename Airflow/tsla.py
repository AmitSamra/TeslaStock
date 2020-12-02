import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import csv
import requests
import os
from airflow.operators.postgres_operator import PostgresOperator
import numpy as np
import pandas as pd
import papermill as pm
import yfinance as yf
from dotenv import load_dotenv


dotenv_local_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path=dotenv_local_path, verbose=True) 

default_args = {
	'owner':'Amit',
	'start_date':datetime.now(),
	'retries':0
}

dag = DAG(
	'tsla',
	default_args = default_args,
	description = 'tsla',
	max_active_runs = 1
)

def etl_yf():
	"""
	Gets stock data from yfinance API.
	Performs ETL.
	"""
	tsla = yf.Ticker("TSLA")
	df_tsla = tsla.history(start="2019-01-01", end="2020-12-01")
	