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

# ----------------------------------------------------------------------------------------------------
# Setup DAG

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

# ----------------------------------------------------------------------------------------------------
# Obtain TSLA stock data from yFinance API

def etl_yfinance():
	"""
	Gets stock data from yfinance API.
	Performs ETL.
	"""
	tsla = yf.Ticker("TSLA")
	df_tsla = df_tsla.reset_index()
	df_tsla.columns = df_tsla.columns.str.replace(' ', '_').str.lower()
	df_tsla['change'] = df_tsla['close']-df_tsla['close'].shift(1)
	df_tsla['percent_change'] = (df_tsla['close']/df_tsla['close'].shift(1))-1
	df_tsla['market_cap'] = df_tsla['close']*df_tsla['volume']/1000000
	df_tsla[['open','high','low','close','change']] = df_tsla[['open','high','low','close','change']].round(2)
	df_tsla[['percent_change']] = df_tsla[['percent_change']].round(4)
	df_tsla[['market_cap']] = df_tsla[['market_cap']].round(1)
	

t1 = PythonOperator(
	task_id = 'etl_yfinance_data',
	python_callable = etl_yfinance,
	provide_context = False,
	dag = dag
)


# ----------------------------------------------------------------------------------------------------
# 