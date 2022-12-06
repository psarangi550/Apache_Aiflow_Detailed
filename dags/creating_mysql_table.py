from airflow import DAG #importing the DAG object in here
from datetime import datetime,timedelta
from airflow.operators.mysql_operator import MySqlOperator
import json

default_args={
    "start_date": datetime(2022,1,1),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

dag=DAG(
    dag_id="create_my_sql_table",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
)

#now here we need to create the DAG in here
create_new_mysql=MySqlOperator(
    task_id="create_new_mysql",
    mysql_conn_id="mysql_new",
    sql="""
        CREATE TABLE IF NOT EXISTS users(
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL
        );
    
    """,
    retries=2,
    retry_delay=timedelta(seconds=2),
    dag=dag
)
