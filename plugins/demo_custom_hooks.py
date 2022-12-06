from airflow.plugins_manager import AirflowPlugin #importing the DAG from airflow
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from typing import List
import logging as log

class PostgresToMysqlTransfer(BaseHook):
    
    def __init__(self):
        print("custom hooks been executing")
        
    def copy_table(self,mysql_conn_id,postgres_conn_id):
        self.log.info("custom connection been made to both mysql as well as postgres")
        postgres_hook=PostgresHook(postgres_conn_id)
        mysql_hook=MySqlHook(mysql_conn_id)
        mysql_conn=mysql_hook.get_conn()
        mysql_cur=mysql_conn.cursor()
        query="select * from city_new ;"
        records=postgres_hook.get_records(query)
        for record in records:
            mysql_cur.execute(f"INSERT INTO city_new (city_name,city_id) VALUES ('{record[0]}','{record[1]}')")
        mysql_conn.commit()
        mysql_cur.close()
        mysql_conn.close()
        return True #returning the True value as the response
    

class DemoHookPlugin(AirflowPlugin):
    name="DemoHookPlugin"
    operators=[]
    hooks=[PostgresToMysqlTransfer]
    sensors:List=[]

