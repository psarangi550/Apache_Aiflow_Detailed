from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime,timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.bash_operator import BashOperator
import json
import pandas as pd

def _process_user():
    print("pandas version",pd.__version__)
    data = [['pratik', 'sarangi' ], ['Abi', 'Sarangi'], ['Rika', 'Sarangi']]
    df=pd.DataFrame(data,columns=["first_name", "last_name"])
    df.to_csv("/usr/local/airflow/output.csv",index=False,header=False)

default_args={
    "start_date":datetime(2022,1,1),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

dag=DAG(
    start_date=datetime(2022,1,1),
    dag_id="extract_api_ingest_table",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False
)

#creating the Task to create the Table 
create_mysql_table=MySqlOperator(
    task_id="create_mysql_table",
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

#task to check the api available or not 

is_user_available=HttpSensor(
    task_id="is_user_available",
    http_conn_id="user_api",
    endpoint="api/"
)

#extracting the User using the SimpleHttpOperator

extract_user=SimpleHttpOperator(
    task_id="extract_user",
    http_conn_id="user_api",
    endpoint="api/",
    method="GET",
    response_check=lambda response:json.loads(response.text),
    log_response=True
)

#now here we need to use the python operator to extract the data into the csv file 
process_user=PythonOperator(
    task_id="process_user",
    python_callable=_process_user
)

#checking the Output_file avaible or not 
check_csv_file=BashOperator(
    task_id="check_csv_file",
    bash_command="shasum /usr/local/airflow/output.csv",
    retries=2,
    retry_delay=timedelta(seconds=5)
)

create_mysql_table >> is_user_available >> extract_user >> process_user >> check_csv_file