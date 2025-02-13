from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import platform
import psutil
import os
import requests
from zipfile import ZipFile
from io import BytesIO
from google.cloud import storage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=2)
}

def example_print(print_text):
  print(print_text)

def get_shapefile(url,bucket_name,blob_name):

  # Initialize GCS client  
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(blob_name)

  # Download and extract
  with ZipFile(BytesIO(requests.get(url).content)) as zip_file:
    for file in zip_file.namelist():
          filename = os.path.basename(file)
          if filename and not file.endswith('/'):  # Skip directories
              # Extract file directly to extract_dir
              with zip_file.open(file) as f:
                blob.upload_from_file(f)
  
  
  
  blob.upload_from_file(response.content)

with DAG(
    'main_dag',
    default_args=default_args,
    description='Primary project DAG',
    # schedule_interval=timedelta(days=1),
    # start_date=datetime(2024, 2, 10),
    catchup=False,
    tags=['prod']
) as dag:

    # Example startup task
    example_startup = PythonOperator(
      task_id='start_up',
      python_callable=example_startup,
      print_text='Starting up...'
    )

    # Get shapefiles 
    get_shapefile_census = PythonOperator(
        task_id='get_shapefile_census',
        python_callable=get_shapefile,
        url='https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nycb2020_24d.zip',
      bucket_name='plavan1-capstone',
      blob_name='raw_shapefiles/census_blocks'
    )

   # Example shutdown task
    example_startup = PythonOperator(
      task_id='shut_down',
      python_callable=example_startup,
      print_text='Shutting down...'
    )

    # Define task dependencies
    example_startup >> get_shapefile_census >> shut_down
