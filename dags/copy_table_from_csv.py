from airflow import DAG #from airflow importing the DAG object in here
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime,timedelta
from airflow.hooks.mysql_hook import MySqlHook
import json
import requests
import csv
from pandas import DataFrame

def _process_usr_data():
    data=[["pratik","sarangi"],["Abi","Sarangi"],["Rika","Sarangi"]]
    df=DataFrame(data=data,columns=["first_name","last_name"])
    df.to_csv("/usr/local/airflow/result.csv",index=False)

def _copy_tbl():
    hook=MySqlHook(mysql_conn_id="mysql_new_usr")
    conn = hook.get_conn()
    cursor = conn.cursor()
    with open("/usr/local/airflow/result.csv","r+") as f:
        csv_reader=csv.reader(f)
        next(csv_reader)
        for row in csv_reader:
            cursor.execute("INSERT INTO new_usr(first_name,last_name) VALUES({},{});".format(f"\'{str(row[0])}\'",f"\'{str(row[1])}\'"))
        conn.commit()


default_args={
    "start_date": datetime(2022,1,1),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
} 

dag=DAG(
    dag_id="copy_table_from_csv",
    start_date=datetime(2022,1,1),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
)

#creating the Table in here
create_usr_tbl=MySqlOperator(
    task_id="create_usr_tbl",
    mysql_conn_id="mysql_new_usr",
    sql="""
        CREATE TABLE IF NOT EXISTS new_usr(
          first_name VARCHAR(50) NOT NULL,
          last_name VARCHAR(50) NOT NULL  
        );
    
    """,
    dag=dag,
    retries=2,
    retry_delay=timedelta(seconds=4)
)

#creating the Task2 to extract the data from the API
extract_usr_data=SimpleHttpOperator(
    task_id="extract_usr_data",
    http_conn_id="user_data",
    dag=dag,
    endpoint="api/",
    method="GET",
    response_check=lambda response:json.loads(response.text),
    log_response=True
)

#using the Python Operator Processing the data
process_usr_data=PythonOperator(
    task_id="process-usr_data",
    python_callable=_process_usr_data,
    dag=dag
)

#here we need to copy the csv content into the table using mysql hooks
copy_tbl=PythonOperator(
    task_id="copy_tbl",
    python_callable=_copy_tbl
)

create_usr_tbl >> extract_usr_data >> process_usr_data >> copy_tbl