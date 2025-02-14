from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import storage
from google.oauth2 import service_account
import requests
from zipfile import ZipFile, BadZipFile
from io import BytesIO
import json
import os
import sys
import logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
sys.path.append('/opt/airflow/plugins')
from create_metrics import create_metrics

# Configure logging
logger = logging.getLogger(__name__)

# Configuration for file downloads
FILE_CONFIG = {
    'census_blocks': {
        'url': 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nycb2020_24d.zip',
        'folder_id': '1ypDODEoBkxrem_1vKIXEUNQ53YoJjiDN'
    },
    'community_districts': {
        'url': 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nycd_24d.zip',
        'folder_id': '1zeKrWm_pWx4egQz37XNFzFvfsEzKYheG'
    },
    'council_districts': {
        'url': 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nycc_24d.zip',
        'folder_id': '1ebeUn6Or1m_D5GI2jDGmO2zb20wXcIQy'
    },
    'school_districts': {
        'url': 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nysd_24d.zip',
        'folder_id': '1rlTB61qxmds6VHWLGhuRMrNNfSsmrrVF'
    },
    'transit_access': {
        'url': 'https://conservancy.umn.edu/bitstreams/ee9a83f2-1630-4e38-96a3-d37f6dc34458/download',
        'folder_id': '1pNWbeItk9eFCs3E423dS1BPZIIKyU_2K'
    },
    'housing_units': {
        'url': 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nychdb_community_24q2_csv.zip',
        'folder_id': '1mVIjaxCs_KZeirJbVP97AE4CHp_KcwXK'
    },
    'school_capacity': {
        'url':'https://data.cityofnewyork.us/api/views/gkd7-3vk7/rows.csv?query=SELECT%0A%20%20%60geo_dist%60%2C%0A%20%20sum(%60bldg_enroll%60)%20AS%20%60sum_bldg_enroll%60%2C%0A%20%20sum(%60target_bldg_cap%60)%20AS%20%60sum_target_bldg_cap%60%0AGROUP%20BY%20%60geo_dist%60&fourfour=gkd7-3vk7&read_from_nbe=true&version=2.1&cacheBust=1704903561&date=20250213&accessType=DOWNLOAD',
        'folder_id': '111_3XXJ8kIT4Ts-1IEPJKf0UDx6TOJNs'
    }
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

creds_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
if not creds_json:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")

creds_dict = json.loads(creds_json)
credentials = service_account.Credentials.from_service_account_info(creds_dict)

def download_and_extract(url: str, folder_id: str) -> None:
    """
    Download a zip file from URL and extract its contents.
    First clears all existing files from the destination folder.
    
    Args:
        url: Source URL for the zip file
        folder_id: destination Google Drive folder
    """
    logger.info(f"Starting download from {url}...")
    
    try:
        # Initialize Google Drive service
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Clear existing files from folder
        logger.info("Clearing existing files from folder...")
        try:
            # List all files in the folder
            results = drive_service.files().list(
                q=f"'{folder_id}' in parents",
                fields="files(id, name)"
            ).execute()
            files = results.get('files', [])
            
            # Delete each file
            for file in files:
                drive_service.files().delete(fileId=file['id']).execute()
                logger.info(f"Deleted file: {file['name']}")
            
            logger.info(f"Cleared {len(files)} files from folder")
            
        except Exception as e:
            logger.error(f"Error clearing folder: {str(e)}")
            raise
        
        # Download zip file
        response = requests.get(url)
        response.raise_for_status()
        
        # Process zip file
        try:
            with ZipFile(BytesIO(response.content)) as zip_file:
                file_count = 0
                for file in zip_file.namelist():
                    filename = os.path.basename(file)
                    if filename and not file.endswith('/'):
                        # Create file metadata
                        file_metadata = {
                            'name': filename,
                            'parents': [folder_id]
                        }
                        
                        # Read and upload file content
                        with zip_file.open(file) as f:
                            content = f.read()
                            fh = BytesIO(content)
                            media = MediaIoBaseUpload(fh, 
                                                    mimetype='application/octet-stream',
                                                    resumable=True)
                            
                            drive_service.files().create(
                                body=file_metadata,
                                media_body=media,
                                fields='id'
                            ).execute()
                            
                            file_count += 1
                            
                logger.info(f"Successfully uploaded {file_count} files to Drive folder")
            
        except BadZipFile:
            # Handle as single file
            filename = 'unknown_file_{}'.format(datetime.now())
            if "Content-Disposition" in response.headers:
                content_disposition = response.headers["Content-Disposition"]
                if "filename=" in content_disposition:
                    filename = content_disposition.split("filename=")[1].strip('"')
            
            # Create file metadata
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            
            # Upload file content
            fh = BytesIO(response.content)
            media = MediaIoBaseUpload(fh, 
                                    mimetype='application/octet-stream',
                                    resumable=True)
            
            drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            logger.info(f"Successfully uploaded 1 file to Drive folder")
            
    except requests.RequestException as e:
        logger.error(f"Failed to download from {url}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        raise

def create_file_tasks(dag):
    """Create tasks for downloading and processing files."""
    with TaskGroup(group_id="file_processing") as file_group:
        for name, config in FILE_CONFIG.items():
            PythonOperator(
                task_id=f"get_file_{name}",
                python_callable=download_and_extract,
                op_kwargs={
                    'url': config['url'],
                    'folder_id': config['folder_id']
                },
                dag=dag
            )
        
        return file_group

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
        python_callable=lambda: logger.info("Starting DAG execution...")
    )
    
    # Create shapefile processing task group
    file_tasks = create_file_tasks(dag)

    create_metrics = PythonOperator(
        task_id='create_metrics',
        python_callable=create_metrics
    )
    
    # End task
    end_task = PythonOperator(
        task_id='shut_down',
        python_callable=lambda: logger.info("DAG execution completed")
    )
    
    # Define task dependencies
    start_task >> file_tasks >> create_metrics >> end_task
