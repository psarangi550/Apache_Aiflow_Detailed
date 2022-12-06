from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta
import pandas as pd
import re

#defining the python callable over here

def remove_dollar(st_id):
    return re.sub("^\$","",st_id).strip()

def _clean_file():
    df=pd.read_csv("/usr/local/airflow/store_files/input.csv")
    df["STORE_ID"]=df["STORE_ID"].map(lambda x:remove_dollar(x))
    df.to_csv("/usr/local/airflow/store_files/output.csv",index=False)


default_args={
    "start_date": datetime(2022,1,1),
    "owner":"airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

#creating the DAG run out in here
dag=DAG(
    dag_id="filtering_and_cleaning",
    template_searchpath="/usr/local/airflow/store_files",
    catchup=False,
    default_args=default_args,
    schedule_interval='@daily'
)

#here we are checking whether the file exists or not
check_avail=BashOperator(
    task_id="check_avail",
    bash_command='shasum /usr/local/airflow/store_files/input.csv',
    retries=2,
    retry_delay=timedelta(seconds=10),
    dag=dag
)

#cleaning the input file if it does exist 
clean_file=PythonOperator(
    task_id="clean_file",
    python_callable=_clean_file,
    dag=dag
)

check_avail >> clean_file
