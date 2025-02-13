from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage
from google.oauth2 import service_account
import requests
from zipfile import ZipFile
from io import BytesIO
import json
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Configuration for file downloads
FILE_CONFIG = {
    'census_blocks': {
        'url': 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nycb2020_24d.zip',
        'blob_path': 'raw_shapefiles/census_blocks'
    },
    'community_districts': {
        'url': 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nycd_24d.zip',
        'blob_path': 'raw_shapefiles/nyc_community_districts'
    },
    'council_districts': {
        'url': 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nycc_24d.zip',
        'blob_path': 'raw_shapefiles/nyc_council_districts'
    },
    'school_districts': {
        'url': 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nysd_24d.zip',
        'blob_path': 'raw_shapefiles/nyc_school_districts'
    },
    'transit_access': {
        'url': 'https://conservancy.umn.edu/bitstreams/ee9a83f2-1630-4e38-96a3-d37f6dc34458/download',
        'blob_path': 'raw_metrics/ny_transit_access'
    },
    'housing_units': {
        'url': 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nychdb_community_24q2_csv.zip',
        'blob_path': 'raw_metrics/nyc_housing_units'
    },
    'school_capacity': {
        'url':'https://data.cityofnewyork.us/api/views/gkd7-3vk7/rows.csv?query=SELECT%0A%20%20%60geo_dist%60%2C%0A%20%20sum(%60bldg_enroll%60)%20AS%20%60sum_bldg_enroll%60%2C%0A%20%20sum(%60target_bldg_cap%60)%20AS%20%60sum_target_bldg_cap%60%0AGROUP%20BY%20%60geo_dist%60&fourfour=gkd7-3vk7&read_from_nbe=true&version=2.1&cacheBust=1704903561&date=20250213&accessType=DOWNLOAD'
        # 'url': '''https://data.cityofnewyork.us/api/views/gkd7-3vk7/rows.csv?query=SELECT%0A%20%20%60\
        # geo_dist%60%2C%0A%20%20sum(%60bldg_enroll%60)%20AS%20%60sum_bldg_enroll%60%2C%0A%20%20sum(%60t\
        # arget_bldg_cap%60)%20AS%20%60sum_target_bldg_cap%60%0AGROUP%20BY%20%60geo_dist%60&fourfour=gkd7\
        # -3vk7&read_from_nbe=true&version=2.1&cacheBust=1704903561&date=20250213&accessType=DOWNLOAD''',
        'blob_path': 'raw_metrics/nyc_school_capacity'
    }
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def get_gcs_client():
    """Initialize and return a GCS client."""
    creds_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not creds_json:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")
    
    creds_dict = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return storage.Client(credentials=credentials)

def download_and_extract_to_gcs(url: str, bucket_name: str, blob_path: str) -> None:
    """
    Download a zip file from URL and extract its contents to GCS.
    
    Args:
        url: Source URL for the zip file
        bucket_name: Target GCS bucket name
        blob_path: Base path for the extracted files in GCS
    """
    logger.info(f"Starting download from {url}")
    
    try:
        # Download zip file
        response = requests.get(url)
        response.raise_for_status()
        
        # Initialize GCS client
        storage_client = get_gcs_client()
        bucket = storage_client.bucket(bucket_name)
        
        # Process zip file
        try:
            with ZipFile(BytesIO(response.content)) as zip_file:
                file_count = 0
                for file in zip_file.namelist():
                    filename = os.path.basename(file)
                    if filename and not file.endswith('/'):
                        blob = bucket.blob(f"{blob_path}/{filename}")
                        
                        with zip_file.open(file) as f:
                            content = f.read()
                            blob.upload_from_string(content)
                            file_count += 1
                            
                logger.info(f"Successfully uploaded {file_count} files to {blob_path}")
            
        except BadZipFile:
            # Handle as single file
            filename = os.path.basename(url)
            blob = bucket.blob(f"{blob_path}/{filename}")
            blob.upload_from_string(response.content) 
            logger.info(f"Successfully uploaded 1 file to {blob_path}")
            
    except requests.RequestException as e:
        logger.error(f"Failed to download from {url}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        raise

def create_shapefile_tasks(dag, bucket_name):
    """Create tasks for downloading and processing shapefiles."""
    with TaskGroup(group_id="shapefile_processing") as shapefile_group:
        for name, config in FILE_CONFIG.items():
            PythonOperator(
                task_id=f"get_shapefile_{name}",
                python_callable=download_and_extract_to_gcs,
                op_kwargs={
                    'url': config['url'],
                    'bucket_name': bucket_name,
                    'blob_path': config['blob_path']
                },
                dag=dag
            )
        
        return shapefile_group

with DAG(
    'main_dag',
    default_args=default_args,
    description='Primary project DAG',
    schedule_interval=None,  # Set to None for manual triggers
    start_date=datetime(2024, 2, 10),
    catchup=False,
    tags=['prod']
) as dag:
    
    # Start task
    start_task = PythonOperator(
        task_id='start_up',
        python_callable=lambda: logger.info("Starting DAG execution")
    )
    
    # Create shapefile processing task group
    file_tasks = create_shapefile_tasks(dag, 'plavan1-capstone')
    
    # End task
    end_task = PythonOperator(
        task_id='shut_down',
        python_callable=lambda: logger.info("DAG execution completed")
    )
    
    # Define task dependencies
    start_task >> file_tasks >> end_task
