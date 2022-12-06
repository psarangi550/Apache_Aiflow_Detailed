from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresToMysqlTransfer
from datetime import datetime,timedelta
from airflow.operators.mysql_operator import MySqlOperator

def _copy_tbl():
    PostgresToMysqlTransfer().copy_table(mysql_conn_id="mysql_conn",postgres_conn_id="postgres_conn")
    #creating the object to call the instance method
    print("All Task Done")


default_args={
    "start_date":datetime(2022,1,1),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

dag=DAG(
    dag_id="custom_hook",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

#here creating 2 postgres task to create a table and insert table with data
task1=PostgresOperator(
    task_id="create_tbl_city",
    postgres_conn_id="postgres_conn",
    dag=dag,
    sql="""
        CREATE TABLE IF NOT EXISTS city_new(
            city_name varchar(25) NOT NULL,
            city_id varchar(10) NOT NULL
        );
    """
)


task2=PostgresOperator(
    task_id="insert_to_city_tbl",
    postgres_conn_id="postgres_conn",
    dag=dag,
    sql="""
        INSERT INTO city_new(city_name, city_id) VALUES ('india','in') ,('united kingdom','uk')
    """
)

task3=PythonOperator(
    task_id="copy_table_task",
    python_callable=_copy_tbl,
    dag=dag
)

task4=MySqlOperator(
    task_id="create_tbl_city_mysql",
    mysql_conn_id="mysql_conn",
    dag=dag,
    sql="""
        CREATE TABLE IF NOT EXISTS city_new(
            city_name varchar(25) NOT NULL,
            city_id varchar(10) NOT NULL
        );
    """
)

task1 >> task2 >> task4 >> task3