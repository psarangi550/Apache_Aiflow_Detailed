from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import re


class DemoTransferOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,source_file,dest_file,delete_list,*args,**kwargs):
        self.source_file = source_file
        self.dest_file=dest_file
        self.delete_list=delete_list
        super().__init__(*args,**kwargs)
        
    
    def execute(self,context):#using the context out here 
        source_file=self.source_file 
        dest_file=self.dest_file 
        delete_file=self.delete_list
        
        logging.info("Source file location: %s" % source_file)
        logging.info("Destination file location: %s" % dest_file)
        logging.info("Delete file location: %s" % delete_file)
        
        fin=open(source_file)
        fout=open(dest_file,"w")
        new_line=""
        for line in fin:
            logging.info("reading the line: %s" % line)
            for word in delete_file:
                logging.info("reading the word: %s" % word)
                line=line.replace(word, "")
            logging.info("writing the word: %s" % line)
            fout.write(line)
        
        fin.close()
        fout.close()

class DemoTransferPlugin(AirflowPlugin):
    name="DemoTransferPlugin"
    operators=[DemoTransferOperator]