from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from refine import _refine_record


def _insert_file_table():
    hook=PostgresHook(postgres_conn_id="postgres_new")
    hook.copy_expert(
        sql="copy store_details_new (STORE_ID,STORE_LOCATION,PRODUCT_CATEGORY,PRODUCT_ID,MRP,CP,DISCOUNT,SP,DATE_ADDED) from stdin with delimiter as ',' CSV HEADER;",
        filename='/usr/local/airflow/store_files/clean_tranactions.csv'
    )
    

default_args={
    "start_date":datetime(2022,1,1),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

dag=DAG(
    dag_id="table_creation_in_postgres",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

table_create=PostgresOperator(
    task_id="table_create",
    postgres_conn_id="postgres_new",
    dag=dag,
    sql="""
        CREATE TABLE IF NOT EXISTS store_details_new(
        STORE_ID VARCHAR NOT NULL,
        STORE_LOCATION VARCHAR NOT NULL,
        PRODUCT_CATEGORY VARCHAR NOT NULL,
        PRODUCT_ID VARCHAR NOT NULL,
        MRP INT NOT NULL,
        CP REAL  NOT NULL,
        DISCOUNT REAL NOT NULL,
        SP REAL NOT NULL,
        DATE_ADDED date NOT NULL
        );
    """
)

refining_recs=PythonOperator(
    task_id="refining_recs",
    python_callable=_refine_record,
    dag=dag
)

insert_table_record=PostgresOperator(
    task_id="insert_table_record",
    postgres_conn_id="postgres_new",
    dag=dag,
    sql="""
        INSERT INTO store_details_new (STORE_ID,STORE_LOCATION,PRODUCT_CATEGORY,PRODUCT_ID,MRP,CP,DISCOUNT,SP,DATE_ADDED)
        VALUES ('YR7220','New York','Electronics',12254943,31,20.77,1.86,29.14,'2019-11-26');
    """
)

insert_file_table=PythonOperator(
    task_id="insert_file_table",
    python_callable=_insert_file_table,
    dag=dag
)


table_create >> refining_recs >> insert_table_record >> insert_file_table