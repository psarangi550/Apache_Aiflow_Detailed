from airflow import DAG #importing the DAG in here
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta

default_args={
    "start_date":datetime(2022,11,24),
    "owner":"Airflow",
    "depends_on_start":False,
    "retries":2,
    "retry_delay":timedelta(seconds=5)
}

with DAG(dag_id="usgae_of_dummy",default_args=default_args,
    schedule_interval=timedelta(hours=6),
    catchup=False,#using the catchup as True for the Backfilling 
    ) as dag:
    
    task1=BashOperator(task_id="task1",bash_command="date")
    task2=BashOperator(task_id="task2",bash_command="date")
    task3=BashOperator(task_id="task3",bash_command="date")
    task4=BashOperator(task_id="task4",bash_command="date")
    task5=BashOperator(task_id="task5",bash_command="date")
    
    td=DummyOperator(task_id="task_dummy")
    
    task6=BashOperator(task_id="task6",bash_command="date")
    task7=BashOperator(task_id="task7",bash_command="date")
    task8=BashOperator(task_id="task8",bash_command="date")
    task9=BashOperator(task_id="task9",bash_command="date")
    task10=BashOperator(task_id="task10",bash_command="date")
    
    [task1,task2,task3,task4,task5] >> td >> [task6,task7,task8,task9,task10]
    