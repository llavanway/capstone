import os
import json
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io
from pathlib import Path
import pandas as pd
import geopandas as gpd
import logging
import tempfile

logger = logging.getLogger(__name__)

def process_shapefiles():

    shapefile_folder_ids = {
        'community_district': '1zeKrWm_pWx4egQz37XNFzFvfsEzKYheG',
        'council_district': '1ebeUn6Or1m_D5GI2jDGmO2zb20wXcIQy',
        'school_district': '1rlTB61qxmds6VHWLGhuRMrNNfSsmrrVF',
        'census_block': '1ypDODEoBkxrem_1vKIXEUNQ53YoJjiDN'
    }
    
    metric_folder_ids = {
        'housing_units': '1mVIjaxCs_KZeirJbVP97AE4CHp_KcwXK',
        'school_capacity': '111_3XXJ8kIT4Ts-1IEPJKf0UDx6TOJNs',
        'transit_access': '1pNWbeItk9eFCs3E423dS1BPZIIKyU_2K'
    }

    final_metrics_folder_id = '1DU72-veBciQ2sxE-hrIDilnM_wxjmx6Y'

    def get_drive_client():
        """Initialize and return a Google Drive client."""
        creds_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not creds_json:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")
        
        creds_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        return build('drive', 'v3', credentials=credentials)

    def clear_folder(service, folder_id):
        """Delete all files in the specified folder."""
        try:
            # List all files in the folder
            results = service.files().list(
                q=f"'{folder_id}' in parents",
                fields="files(id, name)"
            ).execute()
            files = results.get('files', [])
            
            # Delete each file
            for file in files:
                try:
                    service.files().delete(fileId=file['id']).execute()
                    logger.info(f"Deleted file: {file['name']}")
                except Exception as e:
                    logger.error(f"Error deleting file {file['name']}: {e}")
            
            return True
        except Exception as e:
            logger.error(f"Error clearing folder {folder_id}: {e}")
            return False

    def download_file(service, file_id, output_path):
        """Download a file from Google Drive."""
        try:
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            fh.seek(0)
            
            with open(output_path, 'wb') as f:
                f.write(fh.read())
            return True
        except Exception as e:
            logger.error(f"Error downloading file {file_id}: {e}")
            return False

    def read_csv_from_drive(service, folder_name, file_name):
        """
        Read a CSV file from Google Drive into a pandas DataFrame by searching for 
        the file name within the specified folder.
        
        Args:
            service: Google Drive API service instance
            folder_name: Name of the folder to search in
            file_name: Name of the file to download
            
        Returns:
            pandas DataFrame containing the CSV data
        """
        try:
            # First, find the folder ID by name
            folder_query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
            folder_results = service.files().list(q=folder_query, spaces='drive', fields='files(id)').execute()
            folder_items = folder_results.get('files', [])
            
            if not folder_items:
                raise FileNotFoundError(f"Folder '{folder_name}' not found")
                
            folder_id = folder_items[0]['id']
            
            # Then find the file within that folder
            file_query = f"name='{file_name}' and '{folder_id}' in parents"
            file_results = service.files().list(q=file_query, spaces='drive', fields='files(id, mimeType)').execute()
            file_items = file_results.get('files', [])
            
            if not file_items:
                raise FileNotFoundError(f"File '{file_name}' not found in folder '{folder_name}'")
                
            file_id = file_items[0]['id']
            mime_type = file_items[0]['mimeType']
            
            # Handle different file types
            if mime_type == 'application/vnd.google-apps.spreadsheet':
                # For Google Sheets, use the export feature
                request = service.files().export_media(fileId=file_id, mimeType='text/csv')
            else:
                # For regular CSV files, use get_media
                request = service.files().get_media(fileId=file_id)
            
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            fh.seek(0)
            return pd.read_csv(fh)
            
        except Exception as e:
            logger.error(f"Error reading CSV file '{file_name}' from folder '{folder_name}': {e}")
            raise

    drive_service = get_drive_client()
    
    # Create a temporary directory to store shapefile components
    temp_dir = '/tmp/shapefiles'
    os.makedirs(temp_dir, exist_ok=True)
    
    d = {}
    
    for key in shapefile_folder_ids:
        # List files in the shapefile folder
        results = drive_service.files().list(
            q=f"'{shapefile_folder_ids[key]}' in parents",
            fields="files(id, name)").execute()
        files = results.get('files', [])
        
        local_files = []
        for file in files:
            ext = os.path.splitext(file['name'])[1]
            if ext in ['.shp', '.shx', '.dbf', '.prj', '.xml']:
                local_path = f"{temp_dir}/{key}{ext}"
                if download_file(drive_service, file['id'], local_path):
                    local_files.append(local_path)
        
        # Read the shapefile from the local filesystem
        try:
            d[key] = gpd.read_file(f"{temp_dir}/{key}.shp")
        except Exception as e:
            logger.error(f"Error reading shapefile for {key}: {e}")
            continue
        
        # Clean up downloaded files
        for file in local_files:
            try:
                os.remove(file)
            except OSError:
                pass

    # Check shapefile downloads
    logger.info('Shapefile dict keys:')
    logger.info(list(d.keys()))
    
    school_districts = d['school_district']
    council_districts = d['council_district']
    community_districts = d['community_district']
    census_blocks = d['census_block']
    
    # Create a new GeoDataFrame with community district centroids
    community_centroids = community_districts.copy()
    community_centroids.geometry = community_districts.geometry.centroid
    
    # Perform spatial joins using centroids
    comm_school_join = gpd.sjoin(
        community_centroids,
        school_districts,
        how='left',
        predicate='within'
    )
    
    comm_school_join.rename(columns={'index_right':'index_right_prior'}, inplace=True)
    
    council_join = gpd.sjoin(
        comm_school_join,
        council_districts,
        how='left',
        predicate='within'
    )
    
    council_join.rename(columns={'index_right':'index_right_prior'}, inplace=True)
    
    census_join = gpd.sjoin(
        council_join,
        census_blocks,
        how='left',
        predicate='within'
    )
    
    final_districts = census_join[[
        'BoroCD',
        'SchoolDist',
        'CounDist',
        'GEOID',
        'geometry'
    ]]
    
    final_districts = final_districts.merge(
        community_districts[['BoroCD', 'geometry']],
        on='BoroCD',
        suffixes=('_centroid', '')
    )
    
    final_districts = final_districts[~final_districts['BoroCD'].isin([164,226,227,228,355,356,480,481,482,483,484,595])]
    
    final_districts['CensusTract'] = final_districts['GEOID'].str.slice(start=5,stop=11)
    final_districts['CensusID'] = final_districts['GEOID'].str.slice(start=0,stop=11).astype('int64')

    # # separate geometry only to upload to Tableau

    final_geometry = final_districts[['geometry','BoroCD','SchoolDist','CounDist','GEOID','CensusTract']]

    # Convert GeoDataFrame to GeoJSON string
    final_geometry = final_geometry.to_json()
    
    # Create file-like object from GeoJSON string
    file_content = io.BytesIO(final_geometry.encode('utf-8'))
    
    # Prepare file metadata
    file_metadata = {
        'name': 'final_geometry.geojson',
        'mimeType': 'application/geo+json',
        'parents': ['1DU72-veBciQ2sxE-hrIDilnM_wxjmx6Y']
    }
    
    # Create media object from bytes
    media = MediaIoBaseUpload(
        file_content,
        mimetype='application/geo+json',
        resumable=True
    )
    
    # Upload file
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id, webViewLink'
    ).execute()
