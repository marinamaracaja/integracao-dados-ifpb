from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator
import os

from tasks.raw_extract_data_api import raw_extract_data_api
from tasks.stg_transform_data import stg_transform_data
from tasks.trusted_refine_data import trusted_refine_data
from tasks.application_view_data import application_view_data

os.system('pip install fastparquet')
os.system('pip install requests')

with DAG(
    'dag_qualidade_ar',
    start_date = datetime(2024,10,20),
    schedule_interval = '0 8 * * *',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20)
    ) as dag:

  extract_data_api = PythonOperator(
      task_id = 'extract_data_api',
      python_callable = raw_extract_data_api,
      execution_timeout=timedelta(minutes=5)
  )
  transform_data = PythonOperator(
      task_id = 'transform_data',
      python_callable = stg_transform_data
  )
  refine_data = PythonOperator(
      task_id = 'refine_data',
      python_callable = trusted_refine_data
  )
  view_data = PythonOperator(
      task_id = 'view_data',
      python_callable = application_view_data
  )

extract_data_api >> transform_data >> refine_data >> view_data