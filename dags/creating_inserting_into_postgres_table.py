from airflow import DAG
from  airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
from refine import _refine_record

default_args={
    "start_date":datetime(2022,1,1),
    "owner":"airflow",
    "retries":5,
    "retry_delay":timedelta(seconds=5)
}
#creating the DAG Over Here
dag=DAG(
    dag_id="insert_record_from_csv",
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    template_searchpath="/usr/local/airflow/sql_files/"
)
#creating the Task of checking the excel exist or not 
#TASK:-1
check_raw_files=BashOperator(
    task_id="check_raw_files",
    bash_command="shasum /usr/local/airflow/store_files/raw_store_transactions.csv",
    dag=dag,
    retries=2,
    retry_delay=timedelta(seconds=5)
)
#refining the transaction details by python_operator
#TASK:-2
refining_records=PythonOperator(
    task_id="refining_records",
    python_callable=_refine_record,
    dag=dag
)
#creating a table with the help of the PostgresOperator
#TASK:-3
create_sql_table=PostgresOperator(
    task_id="create_sql_table",
    postgres_conn_id="postgres",
    dag=dag,
    sql="create_store_table.sql",
)
#now we can insert data into the table as below
check_clean_files=BashOperator(
    task_id="check_clean_files",
    bash_command="shasum /usr/local/airflow/store_files/clean_tranactions.csv",
    dag=dag,
    retries=2,
    retry_delay=timedelta(seconds=5)
)

#Task-4
insert_sql_table=PostgresOperator(
    task_id="insert_sql_table",
    postgres_conn_id="postgres",
    dag=dag,
    sql="insert_sql_table.sql",
)

# insert_sql_table=BashOperator(
#     bash_command="psql -U airflow "
# )

check_raw_files >> refining_records >> create_sql_table >> check_clean_files >> insert_sql_table