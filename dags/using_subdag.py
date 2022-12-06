from airflow import DAG
from datetime import datetime,timedelta
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

dag_name="using_subdag"

default_args={
    "owner":"Airflow",
    "start_date":datetime(2022,1,1)
}

dag=DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

#task:-1
start_task=DummyOperator(
    task_id="start_task",
    dag=dag,
)

#task2:-
start_another_task=DummyOperator(
    task_id="start_another_task",
    dag=dag,
)

#task3 is the end task

end_task=DummyOperator(
    task_id="end_task",
    dag=dag
)

#now here we are using the subdag using the SubDagOperator
sub_dag_1=SubDagOperator(
    task_id="sub_dag_1",
    subdag=subdag(dag_name,"sub_dag_1",default_args),
    dag=dag
)

#creating another_subdag as below 

sub_dag_2=SubDagOperator(
    task_id="sub_dag_2",
    subdag=subdag(dag_name,"sub_dag_2",default_args),
    dag=dag
)

#defining the task dependency 
start_task >> sub_dag_1 >> start_another_task >> sub_dag_2 >> end_task