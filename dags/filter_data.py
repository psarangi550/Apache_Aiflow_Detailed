import re
import pandas as pd

def filter_userid(val):
    re.sub("^\$","",val).strip()


def _clean_file():
    df=pd.read_csv("/usr/local/airflow/store_files/input.csv")
    df["USER_ID"]=df["USER_ID"].map(lambda x: filter_userid(x))
    df.to_csv("/usr/local/airflow/store_files/output.csv",index=False)
    
    