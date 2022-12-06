import pandas as pd
import re
def _refine_record():
    df=pd.read_csv("/usr/local/airflow/store_files/raw_store_transactions.csv")
    
    def filter_store_loc(store_loc):
        return re.sub("[^\w\s]","",store_loc).strip()

    def remove_dollar(val):
        return re.sub("^\$","",val).strip()

    def remove_str(val):
        all_record=re.findall("\d+",val)
        if all_record:
            return all_record[0]
        return val
    df["STORE_LOCATION"]=df["STORE_LOCATION"].map(lambda x:filter_store_loc(x))
    df["PRODUCT_ID"]=df["PRODUCT_ID"].map(lambda x:remove_str(x))
    for item in ["MRP","CP","DISCOUNT","SP"]:
        df[item]=df[item].map(lambda x:remove_dollar(x))
    df.to_csv("/usr/local/airflow/store_files/clean_tranactions.csv",index=False)

    