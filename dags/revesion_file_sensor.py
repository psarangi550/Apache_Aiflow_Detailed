from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime,timedelta

default_args={
    "start_date":datetime(2022,1,1),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

dag=DAG(
    dag_id="check_file_sensor",
    catchup=False,
    default_args=default_args,
    schedule_interval="@daily"
)

task1=FileSensor(
    task_id="file_exists_or_not",
    dag=dag,
    poke_interval=5,
    timeout=100,
    soft_fail=True,
    fs_conn_id="check_file",
    filepath="/usr/local/airflow/store_files/check.csv"
)
