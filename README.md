# Cloud-Native-E-Commerce-Data-Pipeline
Built an end-to-end medallion data pipeline on GCP using the Kaggle Olist e-commerce dataset

## Overview
This project demonstrates a modern data engineering workflow using Google Cloud Platform (GCP) services. The pipeline covers data ingestion, transformation, and serving, leveraging tools such as Google Cloud Composer (managed Airflow), BigQuery, Dataproc, and GCS.

## Project Structure
- **Code Overview/**: Contains scripts and notebooks for GCP-based workflows.
  - `bigquery_script.sql`: Bigquery SQL scripts for BigQuery operations.
  - `data_ingestion_gcs.py`: Composer/Airflow Python script for ingesting data into Google Cloud Storage.
  - `data_processing_dataproc.ipynb`: Notebook for data processing using Dataproc.
  - `readme.md`: Step-by-step guide for the GCP workflow.
- **notes.txt**: Project notes and observations.
- **sql code/**: Directory for SQL scripts.

## GCP Workflow Summary
1. **Data Ingestion (Bronze Layer)**
   - Use Google Cloud Composer (managed Airflow) to orchestrate data fetching from web and SQL databases.
   - Store raw data in a GCS bucket as the Bronze Layer.
2. **Data Processing & Transformation**
   - Use Dataproc or Databricks for cleaning, transforming, and aggregating data.
   - Store processed data in BigQuery or GCS (Silver/Gold Layers).
3. **Data Serving**
   - Serve data for analytics or downstream applications from BigQuery or other serving layers.

## How to Run
1. Review the notebooks and scripts in the project.
2. Deploy the Airflow DAG in Cloud Composer to automate ingestion.
3. Use Dataproc or Databricks notebooks for transformation.
4. Run SQL scripts in BigQuery as needed.

## Prerequisites
- GCP account with permissions for Composer, GCS, BigQuery, and Dataproc
- Python 3.x
- Jupyter Notebook

## References
- [Google Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Dataproc Documentation](https://cloud.google.com/dataproc/docs)
