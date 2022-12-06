from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args={
    "start_date":datetime(2022,1,1),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

dag=DAG(
    dag_id="checking_file_exists",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

#creating the Task out in here as below
check_avail=BashOperator(
    task_id="check_avail",
    bash_command='shasum /tmp/check_avail.csv',
    retries=2,
    retry_delay=timedelta(seconds=10),
    dag=dag
)