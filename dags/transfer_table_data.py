from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta

def _transfer_data(**kwargs):
    source_hook=PostgresHook(postgres_conn_id="postgres_conn",schema="airflow")
    dest_hook=PostgresHook(postgres_conn_id="postgres_conn",schema="airflow")
    source_conn=source_hook.get_conn(**kwargs)
    dest_conn=dest_hook.get_conn(**kwargs)
    src_cur=source_conn.cursor()
    dest_cur=dest_conn.cursor()
    query="SELECT * FROM city_source ;"
    src_cur.execute(query)
    all_record=src_cur.fetchall()
    if all_record:
        for record in all_record:
            dest_cur.execute(f"INSERT INTO city_dest(city_name,city_id) VALUES ('{record[0]}','{record[1]}')")
        dest_conn.commit()
    src_cur.close()
    dest_cur.close()
    source_conn.close()
    dest_conn.close()
    print("Data Transfer Successfully")

default_args={
    "start_date":datetime(2022,1,1),
    "owner":"Airflow",
    "retries":1,
    "retry_delay":timedelta(seconds=5)
}

dag=DAG(
    dag_id="tranfer_table_data",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
)

#creating the  python callable to use the postgres hooks to transfer the data
tranfer_data=PythonOperator(
    task_id="tranfer_data",
    python_callable=_transfer_data,
    dag=dag
)
