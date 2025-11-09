import pandas as pd
from google.cloud import bigquery
import os

def load_to_bigquery(df,table_id):
    client = bigquery.Client()
    job = client.load_table_from_dataframe(df,table_id)
    job.result()


