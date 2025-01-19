from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import os
import pytz
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta  

_API_KEY = os.getenv("CURRENCY_API_KEY")
gcs_client = storage.Client()
BUCKET_NAME = "de-cwwps-newyork-stock-project"
current_date = datetime.now().strftime("%Y-%m-%d")
base_start_year = 2012 

with DAG(
    dag_id = "stock_fund_pipeline",
    default_args = {
        'owner': 'admin',
    },
    schedule_interval = '@daily',
    start_date = datetime(2025, 1, 18),
    catchup=False,
    tags = ['data-pipeline','mongodb', 'gcs', 'spark', 'bigquery', 'api'],
) as dag:

    # Task 1: Extract stock fund from MongoDB
    def extract_stock_from_mongo(execution_date, **kwargs):
        # Get DAG start date
        dag_start_date = kwargs['dag'].start_date

       # Covert to local timezone
        local_tz = pytz.timezone('Asia/Bangkok')
        start_date_local = dag_start_date.astimezone(local_tz)
        exexution_date_local = execution_date.astimezone(local_tz)
        
        if dag_start_date is None:
            raise ValueError("DAG start_date is not set. Please define it in the DAG configuration.")
        print("DAG start date: ", start_date_local)
        print("Execution date: ", exexution_date_local)

        # Calculate run count (for using mock period data)
        run_count = (exexution_date_local - start_date_local).days
        print(f"Run count: {run_count}")
        
        # Define start year for using in mock period
        print("run count: ", run_count)
        start_year = base_start_year + run_count
        # Check the year range in mock period data
        print("start year: ", start_year)
        # Define range of mock period
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(start_year + 1, 1, 1)

        mongo_hook = MongoHook(mongo_conn_id="mongo_default")
        collection_fundamentals = mongo_hook.get_collection("stock_fundamentals", mongo_db = "newyork_stock")
        print(start_date, end_date)

        fund_query = {
            "Period Ending":{
                "$gte": start_date,
                "$lt": end_date
            }
        }
        
        # Extract data from MongoDB using the queries period
        fund_data = collection_fundamentals.find(fund_query)
        fund_df = pd.DataFrame(list(fund_data))
        print(fund_df.head())

        bucket = gcs_client.bucket(BUCKET_NAME)
        gcs_file_name_fund = f"{current_date}_stock_fundamentals.csv"
        # Write raw data as csv to GCS
        fund_df.to_csv(f"gs://{BUCKET_NAME}/raw_data/{gcs_file_name_fund}", index=False)

    extract_stock_from_mongo = PythonOperator(
        task_id = "extract_stock_from_mongo",
        python_callable = extract_stock_from_mongo,
        provide_context = True
    )

    transfrom_data = SparkSubmitOperator(
        task_id = "transform_data",
        conn_id="spark_default",
        application="./include/scripts/stock_fund_script.py",
        verbose=True,
        jars="https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
        conf={
            "spark.sql.repl.eagerEval.enabled": True,
            "google.cloud.auth.service.account.json.keyfile": "./keys/gcp_key.json",
        }, 
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id = "gcs_to_bq",
        bucket = "de-cwwps-newyork-stock-project",
        source_objects=[f"prep_data/{current_date}_stock_fund_prep.parquet/*"],
        destination_project_dataset_table="de-cwwps-project01.newyork_stock_project.stock_fundamentals",
        source_format="parquet",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
    )

    extract_stock_from_mongo >> transfrom_data >> gcs_to_bq