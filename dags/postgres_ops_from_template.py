from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime,timedelta

default_args={
    "start_date": datetime(2022,1,1),
    "owner":"airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

dag=DAG(
    dag_id="postgres_ops_from_template",
    template_searchpath="/usr/local/airflow/sql_files",
    catchup=False,
    default_args=default_args,
    schedule_interval=timedelta(seconds=5)
)

#creating the Task in here
create_table=PostgresOperator(
    task_id="create_table",
    sql="create_table.sql",
    retries=2,
    postgres_conn_id="postgres",
    retry_delay=timedelta(seconds=5),
    dag=dag
)

