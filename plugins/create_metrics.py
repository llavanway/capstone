import json
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload, MediaInMemoryUpload
import io
import pandas as pd
import logging
from get_drive_client import get_drive_client
from datetime import datetime

logger = logging.getLogger(__name__)

def create_metrics():
    
    metric_folder_ids = {
        'housing_units': '1mVIjaxCs_KZeirJbVP97AE4CHp_KcwXK',
        'school_capacity': '111_3XXJ8kIT4Ts-1IEPJKf0UDx6TOJNs',
        'transit_access': '1pNWbeItk9eFCs3E423dS1BPZIIKyU_2K'
    }

    final_geometry_folder_id = '1PULzSCK0NCcN2b5j7grMaBL6OMvTGJ9G'
    
    final_metrics_folder_id = '1DU72-veBciQ2sxE-hrIDilnM_wxjmx6Y'

    # def clear_folder(service, folder_id):
    #     """Delete all files in the specified folder."""
    #     try:
    #         # List all files in the folder
    #         results = service.files().list(
    #             q=f"'{folder_id}' in parents",
    #             fields="files(id, name)"
    #         ).execute()
    #         files = results.get('files', [])
            
    #         # Delete each file
    #         for file in files:
    #             try:
    #                 service.files().delete(fileId=file['id']).execute()
    #                 logger.info(f"Deleted file: {file['name']}")
    #             except Exception as e:
    #                 logger.error(f"Error deleting file {file['name']}: {e}")
            
    #         return True
    #     except Exception as e:
    #         logger.error(f"Error clearing folder {folder_id}: {e}")
    #         return False

    def read_write_csv_from_drive(service, folder_id, file_name, mode='read', write_content=None):
        """
        Read or write a csv file from Google Drive.
        
        Args:
            service: Google Drive API service instance
            folder_id: ID of the folder to search in
            file_name: Name of the file to download
            mode: read or write
            write_content: file to write
        """
        try:
            # Find the file within the folder using folder ID directly
            file_query = f"name='{file_name}' and '{folder_id}' in parents"
            file_results = service.files().list(q=file_query, spaces='drive', fields='files(id, mimeType)').execute()
            file_items = file_results.get('files', [])
            
            if mode == 'read' and not file_items:
                raise FileNotFoundError(f"File '{file_name}' not found in folder with ID '{folder_id}'")
                
            file_id = file_items[0]['id']
            mime_type = file_items[0]['mimeType']

            if mode == 'read':
            
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

            if mode == 'write':
                # convert to csv for uploading
                content_str = write_content.to_csv(index=False)
                # content_bytes = write_content.to_csv(index=False).encode('utf-8')

                # prepare media
                media = MediaInMemoryUpload(
                content_str.encode('utf-8'),
                mimetype='text/csv',
                resumable=True
                )

                if not file_items:
                    # file not found, write new file
                    file_metadata = {
                    'name': file_name,
                    'parents': [folder_id],
                    'mimeType': 'text/csv'
                    }
            
                    file = service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id'
                    ).execute()

                else:
                    # overwrite existing file
                    updated_file = service.files().update(
                        fileId=file_id,
                        media_body=media
                    ).execute()
            
        except Exception as e:
            logger.error(f"Error handling CSV file '{file_name}' from folder '{folder_id}': {e}; mode was {mode}")
            raise
    
    # Authenticate
    drive_service = get_drive_client()
    
    # Get geometry and create final_data dataframe
    query = f"name = 'final_geometry.geojson' and '{final_geometry_folder_id}' in parents"
    result = drive_service.files().list(q=query, fields='files(id)', pageSize=1).execute()
    final_geometry_file_id = result['files'][0]['id']
    request = drive_service.files().get_media(fileId=final_geometry_file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    _, done = downloader.next_chunk()
    fh.seek(0)
    geojson = json.loads(fh.read().decode('utf-8'))
    final_data = pd.json_normalize(geojson['features'])

    logger.info("final_data converted to pandas dataframe:")
    logger.info(final_data.head())
    logger.info("Available columns in final_data:")
    logger.info(final_data.columns.tolist())

    # Change column names
    logger.info("Renaming columns in final_data...")
    final_data = final_data.rename(columns={'properties.BoroCD': 'BoroCD','properties.SchoolDist': 'SchoolDist', 
                                            'properties.CounDist' :'CounDist', 'properties.GEOID': 'GEOID',
                                            'properties.CensusTract':'CensusTract','properties.CensusID':'CensusID'})
    logger.info("Available columns in final_data:")
    logger.info(final_data.columns.tolist())

    # Get raw metric files
    housing_df = read_write_csv_from_drive(drive_service, metric_folder_ids['housing_units'],
                                           'HousingDB_by_CommunityDistrict.csv')
    school_cap_df = read_write_csv_from_drive(drive_service, metric_folder_ids['school_capacity'],
                                              'Enrollment_Capacity_And_Utilization_Reports_20250223.csv')
    transit_df = read_write_csv_from_drive(drive_service, metric_folder_ids['transit_access'],
                                           'New York_36_transit_census_tract_2022.csv')
    
    # Calculate suitability metrics
    housing_df['units_15_24'] = housing_df[['comp2015', 'comp2016', 'comp2017','comp2018','comp2019',
                                           'comp2020','comp2021','comp2022','comp2023','comp2024']].sum(axis=1)
    
    school_cap_df['SchoolCapacity'] = school_cap_df['Org Target Cap'] - school_cap_df['Org Enroll']
    
    transit_df['JobAccess'] = transit_df['Weighted_average_total_jobs'].astype('int32')
    
    # Join metrics
    final_data = final_data.merge(housing_df[['commntydst','units_15_24']], 
                                  left_on='BoroCD',right_on='commntydst')
    final_data = final_data.merge(school_cap_df[['Geo Dist','SchoolCapacity']], 
                                  left_on='SchoolDist',right_on='Geo Dist')
    final_data = final_data.merge(transit_df[['Census ID','JobAccess']][transit_df['Threshold'] == 45], 
                                  left_on='CensusID',right_on='Census ID')

    # Create borough and interpretable district names
    final_data['boro'] = 'Unknown'
    final_data.loc[(final_data['BoroCD'].astype('str').str[0] == '1') , 'boro'] = 'Manhattan'
    final_data.loc[(final_data['BoroCD'].astype('str').str[0] == '2') , 'boro'] = 'Bronx'
    final_data.loc[(final_data['BoroCD'].astype('str').str[0] == '3') , 'boro'] = 'Brooklyn'
    final_data.loc[(final_data['BoroCD'].astype('str').str[0] == '4') , 'boro'] = 'Queens'
    final_data.loc[(final_data['BoroCD'].astype('str').str[0] == '5') , 'boro'] = 'Staten Island'
    final_data['interpretable_district'] = ''
    final_data['interpretable_district'] = (
        final_data['boro'] + ' Community District ' + final_data['BoroCD'].astype('str').str[1:3]
    )
    final_data['boro_district_number'] = final_data['BoroCD'].astype('str').str[1:3].astype('int')

    # Calculate ranks
    final_data['rank_units_15_24'] = final_data['units_15_24'].rank(method='min',ascending=False)
    final_data['rank_JobAccess'] = final_data['JobAccess'].rank(method='min',ascending=False)
    final_data['rank_SchoolCapacity'] = final_data['SchoolCapacity'].rank(method='min',ascending=False)
    
    # Tertiles
    final_data['tertile_units_15_24'] = pd.qcut(final_data['units_15_24'],q=3,labels=[3,2,1]).astype('int32')
    final_data['tertile_SchoolCapacity'] = pd.qcut(final_data['SchoolCapacity'],q=3,labels=[3,2,1]).astype('int32')
    final_data['tertile_JobAccess'] = pd.qcut(final_data['JobAccess'],q=3,labels=[3,2,1]).astype('int32')
    final_data['JobAccessRating'] = pd.qcut(final_data['JobAccess'],q=3,labels=['Low','Medium','High'])
    final_data['tertile_sum'] = final_data[['tertile_units_15_24','tertile_SchoolCapacity','tertile_JobAccess']].sum(axis=1).astype('int32')

    # Enrollment status
    final_data['SchoolEnrollmentStatus'] = 'Default'
    final_data.loc[final_data['SchoolCapacity'].between(-1000000,1000), 'SchoolEnrollmentStatus'] = 'Over enrolled'
    final_data.loc[final_data['SchoolCapacity'] > 1000, 'SchoolEnrollmentStatus'] = 'Under enrolled'

    # Housing ready
    final_data['HousingReadyDistrict'] = 'No'
    final_data.loc[(final_data['SchoolEnrollmentStatus'] == 'Under enrolled') & 
                   (final_data['JobAccessRating'].isin(['Medium','High'])), 'HousingReadyDistrict'] = 'Yes'
    
    final_data['JobAccessRating5'] = pd.qcut(final_data['JobAccess'],q=5,labels=['Very Low','Low','Medium','High','Very High'])
    final_data['HousingReadyDistrict2'] = 'No'
    final_data.loc[(final_data['SchoolEnrollmentStatus'] == 'Under enrolled') & 
                   (final_data['JobAccessRating'].isin(['Medium','High','Very High'])), 'HousingReadyDistrict2'] = 'Yes'

    # Process timestamp
    final_data['data_process_dt'] = datetime.now()

    # Drop unnecessary geometry columns
    final_data = final_data.drop(columns=['geometry.type', 'geometry.coordinates','type'])

    # # Convert df to csv
    # csv_buffer = io.StringIO()
    # final_data.to_csv(csv_buffer, index=False)
    # csv_buffer.seek(0)
    # csv_string = final_data.to_csv(index=False)

    # Upload or overwrite final_metrics file
    read_write_csv_from_drive(drive_service,final_metrics_folder_id,'final_metrics.csv',mode='write',write_content=final_data)
    
    drive_service.close()
