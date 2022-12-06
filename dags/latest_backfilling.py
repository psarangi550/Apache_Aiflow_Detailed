from airflow import DAG #importing the DAG in here
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from datetime import datetime,timedelta

default_args={
    "start_date":datetime(2022,11,24),
    "owner":"Airflow",
    "depends_on_start":False,
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

with DAG(dag_id="latest_backfilling",default_args=default_args,
    schedule_interval=timedelta(hours=6),
    catchup=True,
    ) as dag:
    task1=LatestOnlyOperator(task_id="task_latest")
    task2=DummyOperator(task_id="task2")
    task3=DummyOperator(task_id="task3")
    task4=DummyOperator(task_id="task4")
    
    task1 >> [task2,task4]
    
    