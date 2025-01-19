# Stock Pipeline: Airflow(Local) + Spark(Local) + GCP for Data Engineering

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
