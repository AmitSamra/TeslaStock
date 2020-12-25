import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
from datetime import datetime, timedelta, date
import papermill as pm
from dotenv import load_dotenv
dotenv_local_path = '/Users/amit/Coding/Projects/TeslaStock/.env'
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

yfinance_path = '/Users/amit/Coding/Projects/TeslaStock/YFinance/yfinance.py'
	
yfinance = BashOperator(
	task_id = 'yfinance_csv',
	bash_command = 'python {}'.format(yfinance_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Obtain headlines from NewsAPI

newsapi_path = '/Users/amit/Coding/Projects/TeslaStock/News/headlines.py'

headlines = BashOperator(
	task_id = 'headlines_csv',
	bash_command = 'python {}'.format(newsapi_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Obtain total article count

newsapi_path = '/Users/amit/Coding/Projects/TeslaStock/News/article_count.py'

article_count = BashOperator(
	task_id = 'article_count_csv',
	bash_command = 'python {}'.format(newsapi_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Obtain total number of tweets mentioning Tesla

tweet_path = '/Users/amit/Coding/Projects/TeslaStock/Twitter/twitter.py'

tweet_count = BashOperator(
	task_id = 'tweet_count_csv',
	bash_command = 'python {}'.format(twitter_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Perform NLP on headlines

nlp_path = '/Users/amit/Coding/Projects/TeslaStock/NLP/nlp.py'

nlp = BashOperator(
	task_id = 'sentiment_score_csv',
	bash_command = 'python {}'.format(twitter_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Combine stock price, article count & sentiment score into one df

final_df_path = '/Users/amit/Coding/Projects/TeslaStock/Regression/final_df.py'

final_df = BashOperator(
	task_id = 'final_df',
	bash_command = 'python {}'.format(final_df_path)
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Run regression notebook

regression_notebook_in_path = '/Users/amit/Coding/Projects/TeslaStock/Regression/regression_input.ipynb'
regression_notebook_out_path = '/Users/amit/Coding/Projects/TeslaStock/Regression/regression_output.ipynb'

def run_regression_notebook():
	pm.execute_notebook(regression_notebook_in_path, regression_notebook_out_path)

regression_notebook = PythonOperator(
	task_id = 'regression_notebook',
	python_callable = run_regression_notebook
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Dependencies

yfinance >> headlines >> article_count >> tweet_count >> nlp >> final_df >> regression_notebook
