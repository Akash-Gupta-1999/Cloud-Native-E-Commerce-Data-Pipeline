import os
import tempfile
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator

# --- CONFIGURATION ---
METADATA_URL = "https://github.com/Akash-Gupta-1999/ForEachInput.json"
ABSOLUTE_URL = "https://github.com/Akash-Gupta-1999/"
GCS_BUCKET_NAME = "otis-dataset-project"

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_web_files(metadata_url, bucket_name):
    """
    Reads metadata from a URL, parses it for file paths, downloads those files,
    and uploads them directly to GCS using a temporary file to save memory.
    """
    # 1. Fetch the metadata file
    print(f"Fetching metadata from: {metadata_url}")
    response = requests.get(metadata_url)
    response.raise_for_status() # Fails the task if the HTTP request fails
    
    # 2. Parse the metadata (Assuming it's JSON like: {"files": ["http://link1.csv", "http://link2.csv"]})
    # NOTE: Adjust this parsing logic based on your actual metadata format (CSV, Text, etc.)
    metadata = response.json()
    
    if not metadata:
        print("No files found in metadata.")
        return

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

    # 3. Fetch each file and upload to GCS
    for block in metadata:
        relative_url = block.get("csv_relative_url")
        file_name = block.get("file_name")
        # destination_blob = f"{gcs_prefix}/{file_name}"
        
        file_url = ABSOLUTE_URL + relative_url
        print(f"Downloading {file_name} with {file_url}...")
        
        # Stream the download to a temporary file so large files don't crash the Airflow worker (OOM)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            with requests.get(file_url, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
            temp_file_path = temp_file.name

        try:
            # Upload the temp file to GCS
            print(f"Uploading to gs://{bucket_name}/Bronze/{file_name}")
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=f'Bronze/{file_name}', # You can customize the destination path in GCS
                filename=temp_file_path
            )
        finally:
            # Clean up the temporary file from the Airflow worker
            os.remove(temp_file_path)

with DAG(
    dag_id='web_and_sql_to_gcs_pipeline',
    default_args=default_args,
    description='Fetch web files via metadata, then export SQL to GCS',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['gcs', 'ingestion'],
) as dag:

    # Task 1: Fetch metadata, download web files, and push to GCS
    fetch_web_data_task = PythonOperator(
        task_id='fetch_web_data_to_gcs',
        python_callable=process_web_files,
        op_kwargs={
            'metadata_url': METADATA_URL,
            'bucket_name': GCS_BUCKET_NAME
        }
    )

    # Task 2: Fetch data from SQL DB and write to GCS
    # NOTE: If you use MySQL, change this to MySQLToGCSOperator
    # --- TASK 2: MySQL to GCS ---
    fetch_mysql_to_gcs_task = MySQLToGCSOperator(
        task_id='fetch_mysql_to_gcs',
        mysql_conn_id='my_mysql_connection',      # The name of the connection in Airflow UI
        gcp_conn_id='google_cloud_default',       # The name of the connection in Airflow UI
        
        # Example SQL query
        sql='SELECT * FROM OlistProjectDB_streetwalk.olist_order_payments;',    
        
        bucket=GCS_BUCKET_NAME,
        
        # Example output file: "raw_data/mysql_extracts/sales_orders_2023-10-25.csv"
        filename=f'Bronze/olist_order_payments', 
        
        export_format='csv',
        # You can change this to 'json' or 'parquet' if preferred
    )

    # Define task dependencies (Run Web extraction first, then SQL extraction)
    fetch_web_data_task >> fetch_mysql_to_gcs_task