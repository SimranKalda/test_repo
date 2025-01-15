#!/usr/bin/env python3

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'simran',
    'depends_on_past': False,
    'email': ['simran.kalda96@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=40),
}

with DAG(
    'de_dq_v12_automation',
    default_args=default_args,
    description='DAG to automate DE_DQ_V4.py script',
    schedule_interval='@daily',  
    start_date=datetime(2024, 1, 1),  
    catchup=False,
    tags=['data-engineering', 'automation'],
) as dag:


    run_de_dq_script = BashOperator(
    task_id='run_de_dq_script',
    bash_command='python3 /Users/simran28/UE_Courses/Projects/DataEngineeringProject/TradeDataAnalysis/Scripts/DE_Worldbank_pipeline_V12.py',
    dag=dag,
)

    run_de_dq_script
