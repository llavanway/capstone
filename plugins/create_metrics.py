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

logger = logging.getLogger(__name__)

def create_metrics():

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

    def read_csv_from_drive(service, file_id):
        """
        Read a CSV file from Google Drive into a pandas DataFrame.
        Handles both regular CSV files and Google Sheets.
        """
        try:
            # First, get the file metadata to check its mimeType
            file_metadata = service.files().get(fileId=file_id, fields='mimeType').execute()
            mime_type = file_metadata.get('mimeType', '')
    
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
            logger.error(f"Error reading CSV file {file_id}: {e}")
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
    
    final_data = final_districts.copy()
    
    # Download and read metric files
    housing_df = read_csv_from_drive(drive_service, metric_folder_ids['housing_units'])
    school_cap_df = read_csv_from_drive(drive_service, metric_folder_ids['school_capacity'])
    transit_df = read_csv_from_drive(drive_service, metric_folder_ids['transit_access'])
    
    # Process metrics
    housing_df['units_15_24'] = housing_df[['comp2015', 'comp2016', 'comp2017','comp2018','comp2019',
                                           'comp2020','comp2021','comp2022','comp2023','comp2024']].sum(axis=1)
    
    school_cap_df['SchoolCapacity'] = school_cap_df['Target Bldg Cap'] - school_cap_df['Bldg Enroll']
    
    transit_df['JobAccess'] = transit_df['Weighted_average_total_jobs'].astype('int32')
    
    # Join metrics
    final_data = final_data.merge(housing_df[['commntydst','units_15_24']], left_on='BoroCD',right_on='commntydst')
    final_data = final_data.merge(school_cap_df[['Geo Dist','SchoolCapacity']], left_on='SchoolDist',right_on='Geo Dist')
    final_data = final_data.merge(transit_df[['Census ID','JobAccess']][transit_df['Threshold'] == 45], left_on='CensusID',right_on='Census ID')
    
    # Calculate metrics
    final_data['tertile_units_15_24'] = pd.qcut(final_data['units_15_24'],q=3,labels=[3,2,1]).astype('int32')
    final_data['tertile_SchoolCapacity'] = pd.qcut(final_data['SchoolCapacity'],q=3,labels=[3,2,1]).astype('int32')
    final_data['tertile_JobAccess'] = pd.qcut(final_data['JobAccess'],q=3,labels=[3,2,1]).astype('int32')
    final_data['JobAccessRating'] = pd.qcut(final_data['JobAccess'],q=3,labels=['Low','Medium','High'])
    final_data['tertile_sum'] = final_data[['tertile_units_15_24','tertile_SchoolCapacity','tertile_JobAccess']].sum(axis=1).astype('int32')
    
    final_data['SchoolEnrollmentStatus'] = 'Default'
    final_data.loc[final_data['SchoolCapacity'].between(-1000000,1000), 'SchoolEnrollmentStatus'] = 'Over enrolled'
    final_data.loc[final_data['SchoolCapacity'] > 1000, 'SchoolEnrollmentStatus'] = 'Under enrolled'
    
    final_data['HousingReadyDistrict'] = 'No'
    final_data.loc[(final_data['SchoolEnrollmentStatus'] == 'Under enrolled') & 
                   (final_data['JobAccessRating'].isin(['Medium','High'])), 'HousingReadyDistrict'] = 'Yes'
    
    final_data['JobAccessRating5'] = pd.qcut(final_data['JobAccess'],q=5,labels=['Very Low','Low','Medium','High','Very High'])
    final_data['HousingReadyDistrict2'] = 'No'
    final_data.loc[(final_data['SchoolEnrollmentStatus'] == 'Under enrolled') & 
                   (final_data['JobAccessRating'].isin(['Medium','High','Very High'])), 'HousingReadyDistrict2'] = 'Yes'
    
    # Clear the final metrics folder before uploading
    if not clear_folder(drive_service, final_metrics_folder_id):
        logger.error("Failed to clear final metrics folder")
        return
    
    # Convert to CSV and upload to Drive
    csv_buffer = io.StringIO()
    final_data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    media = MediaIoBaseUpload(
        io.BytesIO(csv_buffer.getvalue().encode()),
        mimetype='text/csv',
        resumable=True
    )
    
    # Create the file in the final_metrics folder
    file_metadata = {
        'name': 'final_metrics.csv',
        'parents': [final_metrics_folder_id]
    }
    
    drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    
    drive_service.close()
