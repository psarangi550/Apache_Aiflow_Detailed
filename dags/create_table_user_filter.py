from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta
import pandas as pd
from filter_data import _clean_file,filter_userid

default_args={
    "start_date": datetime(2022,1,1),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

dag=DAG(
    dag_id="create_table_user_filter",
    schedule_interval="@daily",
    catchup=False,
    template_searchpath="/usr/local/airflow/sql_files",
    default_args=default_args
)

#here is the Task1 in here
check_file=BashOperator(
    task_id="check_file",
    bash_command="shasum /usr/local/airflow/store_files/input.csv",
    dag=dag,
    retries=2,
    retry_delay=timedelta(seconds=5)
)

#cleaning the entry
clean_file=PythonOperator(
    task_id="clean_file",
    python_callable=_clean_file,
    dag=dag,
)

#inserting the connection to DB
create_tbl=PostgresOperator(
    task_id="create_tbl",
    postgres_conn_id="postgres",
    sql="create_table.sql",
    dag=dag
)


check_file >> clean_file >> create_tbl

