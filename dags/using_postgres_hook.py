from airflow import DAG #importing the DAG object from the airflow module 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
from airflow.operators.postgres_operator import PostgresOperator

def _task2(**kwargs):
    hook=PostgresHook(postgres_conn_id="postgres_conn")
    conn=hook.get_conn()
    cur=conn.cursor()
    cur.execute("INSERT INTO users_avail (firstname,lastname) VALUES ('pratik','sarangi')")
    conn.commit()
    cur.close()
    conn.close()

default_args={
    "start_date":datetime(2022,1,1),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=10)
}

dag=DAG(
    dag_id="postgres_hook",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
)

task1=PostgresOperator(
    task_id="task1_postgres",
    postgres_conn_id="postgres_conn",
    sql="""
        CREATE TABLE IF NOT EXISTS users_avail(
            firstname VARCHAR(10) NOT NULL,
            lastname VARCHAR(10) NOT NULL
        );
    """,
    dag=dag
)

task2=PythonOperator(
    task_id="task2_python",
    python_callable=_task2,
    dag=dag,
    provide_context=True
)

task1 >> task2


