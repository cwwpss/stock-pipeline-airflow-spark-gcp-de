# Stock Pipeline: Airflow(Local) + Spark(Local) + GCP for Data Engineering
Chawalwit P.

## Overview of Project
This project demonstrates a complete Data Engineering Pipeline for processing historical stock and fundamental data.
It incorporates **Airflow** for orchestration, **PySpark** for data processing, and integrates with **Google Cloud Platform (GCP)** services such as **Google Cloud Storage (GCS)** and **BigQuery**.
The orchestration and data processing components (Airflow and Spark) are run locally using **Docker**.

## Key Features of Project
- Data Extraction:
  - Extract stock prices and stock fundamentals from **MongoDB**.
  - Extract exchange rates from a **public API**.
  - Extract stock sector and details from a local **CSV file**.
  - Load the extracted data to Google Cloud Storage as raw files (CSV).
- Data Transformation:
  - Use **PySpark** (running locally in Docker) to clean and process data (e.g., merging and renaming columns).
  - Compute stock technical metrics, such as SMA (Simple Moving Average) and RSI (Relative Strength Index).
- Data Storage:
  - Save the processed data as **Parquet** files in **Google Cloud Storage**.
  - Load data into **BigQuery** for analysis.
- Visualization:
  - Build a dashboard in **Looker Studio** to analyze stock trends and fundamental trends, including revenue and gross profit.
- Orchestration:
  - Use Apache Airflow (running locally in Docker) to automate the pipeline and monitor tasks.
