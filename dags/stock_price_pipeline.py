from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import os
import time
import pytz
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta  
from dateutil.relativedelta import relativedelta

_API_KEY = os.getenv("CURRENCY_API_KEY")
gcs_client = storage.Client()
BUCKET_NAME = "de-cwwps-newyork-stock-project"
current_date = datetime.now().strftime("%Y-%m-%d")
base_start_year = 2012  

with DAG(
    dag_id = "stock_price_pipeline",
    default_args = {
        'owner': 'admin',
    },
    schedule_interval = '@daily',
    start_date = datetime(2025, 1, 18),
    catchup=False,
    tags = ['data-pipeline','mongodb', 'gcs', 'spark', 'bigquery', 'api'],
) as dag:
        
    # Get currency exchange data from api
    def get_api_data(execution_date, **kwargs):
        # Get DAG start date
        dag_start_date = kwargs['dag'].start_date

        # Get execution date and covert to local timezone
        local_tz = pytz.timezone('Asia/Bangkok')
        start_date_local = dag_start_date.astimezone(local_tz)
        exexution_date_local = execution_date.astimezone(local_tz)
        run_count = (exexution_date_local - start_date_local).days
        start_year = base_start_year + run_count
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(start_year + 1, 1, 1)

        all_data = []

        # Get currency exchange data
        while start_date <= end_date:
            url = f"https://api.currencyapi.com/v3/historical?apikey={_API_KEY}&currencies=EUR%2CTHB&date={start_date}"
            res = requests.get(url)
            data = res.json()
            print(f"data {start_date}: ", data)
            all_data.append({
                "exchange_date": start_date.strftime('%Y-%m-%d'),
                "curency_base": "USD",
                "usd_value": 1,
                "curency_target": "THB",
                "exchange_rate": data["data"]["THB"]['value']
            })
            time.sleep(2)
            start_date += relativedelta(months=1)

        currency_df = pd.DataFrame(all_data)

        currency_df.to_csv(f"gs://de-cwwps-newyork-stock-project/{current_date}_currency_exchange.csv", index=False)

    get_api_data = PythonOperator(
        task_id = "get_api_data",
        python_callable=get_api_data,
        provide_context=True
    )

    def extract_stock_from_mongo(execution_date, **kwargs):
        # Get DAG start date
        dag_start_date = kwargs['dag'].start_date

        # Convert to local timezone
        local_tz = pytz.timezone('Asia/Bangkok')
        start_date_local = dag_start_date.astimezone(local_tz)
        exexution_date_local = execution_date.astimezone(local_tz)
        
        if dag_start_date is None:
            raise ValueError("DAG start_date is not define.")
        print("DAG start date: ", start_date_local)
        print("\n")
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
        collection_price = mongo_hook.get_collection("stock_prices", mongo_db = "newyork_stock")
        print(start_date, end_date)
        
        price_query = {
            "date": {'$gte': start_date, 
                     '$lt': end_date}
        }

        # Extract data from MongoDB using the queries period
        price_data = collection_price.find(price_query)
        price_df = pd.DataFrame(list(price_data))
        print(price_df.head())

        bucket = gcs_client.bucket(BUCKET_NAME)
        gcs_file_name_price = f"{current_date}_stock_prices.csv"
        # Write raw data as csv to GCS
        price_df.to_csv(f"gs://{BUCKET_NAME}/raw_data/{gcs_file_name_price}", index=False)

    extract_stock_from_mongo = PythonOperator(
        task_id = "extract_stock_from_mongo",
        python_callable = extract_stock_from_mongo,
        provide_context = True
    )

    transfrom_data = SparkSubmitOperator(
        task_id = "transform_data",
        conn_id="spark_default",
        application="./include/scripts/stock_price_script.py",
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
        source_objects=[f"prep_data/{current_date}_stock_price_prep.parquet/*"],
        destination_project_dataset_table="de-cwwps-project01.newyork_stock_project.stock_price",
        source_format="parquet",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
    )

    [extract_stock_from_mongo, get_api_data] >> transfrom_data >> gcs_to_bq