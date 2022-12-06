from airflow.plugins_manager import AirflowPlugin
from airflow.operators.sensors import BaseSensorOperator
from airflow.models import BaseOperator
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.utils.decorators import apply_defaults
import os
import logging as log


class FileCounterSensor(BaseSensorOperator):
    
    @apply_defaults
    def __init__(self,dir_path,fs_conn_id, *args, **kwargs):
        self.dir_path = dir_path
        self.fs_conn_id = fs_conn_id
        super().__init__(*args, **kwargs)
        
    def poke(self, context):
        file_path=FSHook(self.fs_conn_id)
        base_dir=file_path.get_path()
        full_dir_path=os.path.join(base_dir, self.dir_path)
        self.log.info("Full Path for the file is",full_dir_path)
        try:
            for dir,subdir,files in os.walk(full_dir_path):
                if len(files)==5:
                    return True
        except OSError:
            return False
        return False



class DemoFilePlugin(AirflowPlugin):
    name="DemoFilePlugin"
    operators=[]
    sensors=[FileCounterSensor]
    hooks=[]
    
    