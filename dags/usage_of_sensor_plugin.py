from airflow import DAG
from airflow.operators import FileCounterSensor
from datetime import datetime,timedelta


default_args={
    "start_date":datetime(2022,1,1),
    "owner":"Airflow",
}

dag=DAG(
    dag_id="file_sensing_counter",
    catchup=False,
    schedule_interval="@daily",
    default_args=default_args
)

task1=FileCounterSensor(
    task_id="sensing_file",
    dag=dag,
    dir_path="/usr/local/airflow/sql_files/",
    fs_conn_id="file_conn",
    poke_interval=10,
    timeout=200,
    soft_fail=True
)