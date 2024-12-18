from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

from tasks.raw_extract_data_api import processar_planilha_mapbiomas
from tasks.stg_transform_data import stg_transform_data
from tasks.trusted_refine_data import trusted_refine_data
from tasks.application_view_data import application_view_data

# Instalação de pacotes necessários (somente necessário se não estiverem instalados no ambiente)
os.system('pip install fastparquet')
os.system('pip install requests')

with DAG(
    'dag_qualidade_ar',
    start_date=datetime(2024, 10, 20),
    schedule_interval='0 8 * * *',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30)
) as dag:

    # Operador para extrair dados da API Mapbiomas
    process_planilha = PythonOperator(
        task_id='process_planilha_mapbiomas',
        python_callable=processar_planilha_mapbiomas,
        execution_timeout=timedelta(minutes=15)
    )

    # Operador para transformar dados
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=stg_transform_data,
        execution_timeout=timedelta(minutes=30)
    )

    # Operador para refinar dados
    refine_data = PythonOperator(
        task_id='refine_data',
        python_callable=trusted_refine_data,
        execution_timeout=timedelta(minutes=10)
    )

    # Operador para visualizar dados
    view_data = PythonOperator(
        task_id='view_data',
        python_callable=application_view_data,
        execution_timeout=timedelta(minutes=10)
    )

    # Definindo as dependências das tarefas
    process_planilha >> transform_data >> refine_data >> view_data