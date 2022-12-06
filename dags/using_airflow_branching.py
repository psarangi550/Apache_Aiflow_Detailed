from airflow import DAG #importing the DAG object in here
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta

def _push_function(**kwargs):
    number=4,
    ti=kwargs['ti']
    ti.xcom_push(key="num",value=number)

def _even_odd(**kwargs):
    ti=kwargs['ti']
    pulled_number=ti.xcom_pull(key="num")
    if pulled_number[0]%2==0:
        return 'even_task'
    else:
        return 'odd_task'

default_args={
    "start_date":datetime(2022,1,1),
    "owner":"Airflow",
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

dag=DAG(
    dag_id="using_airflow_branching",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
)

#here produce ramdom number using the xcom_push and xcom_pull

push_number=PythonOperator(
    task_id="push_number",
    python_callable=_push_function,
    provide_context=True,
    dag=dag
)

#checking the value is of even or odd and redirecting accordingly

even_odd=BranchPythonOperator(
    task_id="even_odd",
    python_callable=_even_odd,
    provide_context=True,
    dag=dag
) 

even_task=BashOperator(
    task_id="even_task",
    bash_command="echo 'Got even number' ",
    dag=dag
)

odd_task=BashOperator(
    task_id="odd_task",
    bash_command="echo 'Got Odd number' ",
    dag=dag
)

#here defining the task dependency in here 
push_number >> even_odd >> [odd_task,even_task]