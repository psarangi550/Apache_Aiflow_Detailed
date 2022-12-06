from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators import DemoTransferOperator
from airflow.operators.bash_operator import BashOperator

dag=DAG(
    dag_id="demo_transfer_usage",
    start_date=datetime(2022,1,1),
    schedule_interval="@daily",
    catchup=False
)

#task:1:--

task1=DemoTransferOperator(
    task_id="usage_file",
    source_file="/usr/local/airflow/plugins/input.txt",
    dest_file="/usr/local/airflow/plugins/output.txt",
    delete_list=["Airflow","is"],
    dag=dag
)