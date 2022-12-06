from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta


def _push_data(**kwargs):
    message="How are you"
    ti=kwargs['ti']
    ti.xcom_push(key="message",value=message)
    

def _pull_data(**kwargs):
    ti=kwargs['ti']
    pulled_val=ti.xcom_pull(key="message")
    print(f"fetched value {pull_data}")

default_args={
    "start_date":datetime(2022,1,1,),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=10)
}


dag=DAG(
    dag_id="xcom_push_pull",
    default_args=default_args,
    catchup=False,
    schedule_interval="@daily"
)

#task t1 to push the Data 
push_data=PythonOperator(
    task_id="push_data",
    python_callable=_push_data,
    dag=dag,
    provide_context=True
)

#task t1 to pull the Data 
pull_data=PythonOperator(
    task_id="pull_data",
    python_callable=_pull_data,
    dag=dag,
    provide_context=True
)


push_data >> pull_data