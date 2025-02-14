import os
import json
from google.cloud import storage
from google.oauth2 import service_account
import io
from pathlib import Path
import pandas as pd
import geopandas as gpd
import logging

logger = logging.getLogger(__name__)

def create_metrics():
  
  def get_gcs_client():
      """Initialize and return a GCS client."""
      creds_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
      if not creds_json:
          raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")
      
      creds_dict = json.loads(creds_json)
      credentials = service_account.Credentials.from_service_account_info(creds_dict)
      return storage.Client(credentials=credentials)
  
  gcp_bucket = 'plavan1-capstone'
  
  shapefile_blobs = {
      'community_district': 'raw_shapefiles/nyc_community_districts/nycd',
      'council_district': 'raw_shapefiles/nyc_council_districts/nycc',
      'school_district': 'raw_shapefiles/nyc_school_districts/nysd',
      'census_block': 'raw_shapefiles/census_blocks/nycb2020'
  }
  
  metric_blobs = {
      'housing_units': 'raw_metrics/nyc_housing_units/HousingDB_by_CommunityDistrict.csv',
      'school_capacity': 'raw_metrics/nyc_school_capacity/Enrollment_Capacity_And_Utilization_Reports_20250213.csv',
      'transit_access': 'raw_metrics/ny_transit_access/New York_36_transit_census_tract_2022.csv'
  }

  storage_client = get_gcs_client()
  bucket = storage_client.bucket(gcp_bucket)

  # Create a temporary directory to store shapefile components
  temp_dir = '/tmp/shapefiles'
  os.makedirs(temp_dir, exist_ok=True)
  
  d = {}
  
  for key in shapefile_blobs:
    # Download all shapefile components (.shp, .shx, .dbf, .prj, .xml)
    base_path = shapefile_blobs[key]
    local_files = []
    
    for ext in ['.shp', '.shx', '.dbf', '.prj','.xml']:
        blob = bucket.blob(f"{base_path}{ext}")
        local_path = f"{temp_dir}/{key}{ext}"
        
        try:
            blob.download_to_filename(local_path)
            local_files.append(local_path)
        except Exception as e:
            print(f"Warning: Could not download {ext} file: {e}")
    
    # Read the shapefile from the local filesystem
    try:
        d[key] = gpd.read_file(f"{temp_dir}/{key}.shp")
    except Exception as e:
        print(f"Error reading shapefile for {key}: {e}")
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
  # Join with school districts
  comm_school_join = gpd.sjoin(
      community_centroids,
      school_districts,
      how='left',
      predicate='within'
  )
  
  # Rename index_right to avoid error with later join
  comm_school_join.rename(columns={'index_right':'index_right_prior'},inplace=True)
  
  # Join with council districts
  council_join = gpd.sjoin(
      comm_school_join,
      council_districts,
      how='left',
      predicate='within'
  )
  
  # Rename index_right to avoid error with later join
  council_join.rename(columns={'index_right':'index_right_prior'},inplace=True)
  
  # Join with census blocks
  census_join = gpd.sjoin(
      council_join,
      census_blocks,
      how='left',
      predicate='within'
  )
  
  # Select and rename relevant columns
  final_districts = census_join[[
      'BoroCD',
      'SchoolDist',
      'CounDist',
      'GEOID',
      'geometry'  # This will be the centroid geometry
  ]]
  
  # Optional: Add original community district geometry back if needed
  final_districts = final_districts.merge(
      community_districts[['BoroCD', 'geometry']],
      on='BoroCD',
      suffixes=('_centroid', '')
  )
  
  # Drop invalid community disricts representing parks, etc
  final_districts = final_districts[~final_districts['BoroCD'].isin([164,226,227,228,355,356,480,481,482,483,484,595])]
  
  # Add census tract
  final_districts['CensusTract'] = final_districts['GEOID'].str.slice(start=5,stop=11)
  final_districts['CensusID'] = final_districts['GEOID'].str.slice(start=0,stop=11).astype('int64')
  
  
  # Begin joining metrics
  final_data = final_districts.copy()
  
  # download metrics
  housing_df = pd.read_csv(io.BytesIO(get_gcs_client().bucket(gcp_bucket).blob(metric_blobs['housing_units']).download_as_string()))
  school_cap_df = pd.read_csv(io.BytesIO(get_gcs_client().bucket(gcp_bucket).blob(metric_blobs['school_capacity']).download_as_string()))
  transit_df = pd.read_csv(io.BytesIO(get_gcs_client().bucket(gcp_bucket).blob(metric_blobs['transit_access']).download_as_string()))
  
  # add total units metric
  housing_df['units_15_24'] = housing_df[['comp2015', 'comp2016', 'comp2017','comp2018','comp2019',
                                          'comp2020','comp2021','comp2022','comp2023','comp2024',]].sum(axis=1)
  
  # add school capacity metric
  school_cap_df['SchoolCapacity'] = school_cap_df['Target Bldg Cap'] - school_cap_df['Bldg Enroll']
  
  # adjust dtype
  transit_df['JobAccess'] = transit_df['Weighted_average_total_jobs'].astype('int32')
  
  # keys: BoroCD, SchoolDist, CounDist, GEOID, censusTract
  # join housing
  final_data = final_data.merge(housing_df[['commntydst','units_15_24']], left_on='BoroCD',right_on='commntydst')
  
  # join school capacity
  final_data = final_data.merge(school_cap_df[['Geo Dist','SchoolCapacity']], left_on='SchoolDist',right_on='Geo Dist')
  
  # join transit
  final_data = final_data.merge(transit_df[['Census ID','JobAccess']][transit_df['Threshold'] == 45], left_on='CensusID',right_on='Census ID')
  
  # calculate tertiles
  final_data['tertile_units_15_24'] = pd.qcut(final_data['units_15_24'],q=3,labels=[3,2,1])
  final_data['tertile_SchoolCapacity'] = pd.qcut(final_data['SchoolCapacity'],q=3,labels=[3,2,1])
  final_data['tertile_JobAccess'] = pd.qcut(final_data['JobAccess'],q=3,labels=[3,2,1])
  final_data['JobAccessRating'] = pd.qcut(final_data['JobAccess'],q=3,labels=['Low','Medium','High'])
  final_data['tertile_sum'] = final_data[['tertile_units_15_24','tertile_SchoolCapacity','tertile_JobAccess']].sum(axis=1).astype('int32')
  
  # additional metrics
  final_data['SchoolEnrollmentStatus'] = 'Default'
  final_data.loc[final_data['SchoolCapacity'].between(-1000000,1000), 'SchoolEnrollmentStatus'] = 'Over enrolled'
  final_data.loc[final_data['SchoolCapacity'] > 1000, 'SchoolEnrollmentStatus'] = 'Under enrolled'
  
  final_data['HousingReadyDistrict'] = 'No'
  final_data.loc[(final_data['SchoolEnrollmentStatus'] == 'Under enrolled') & (final_data['JobAccessRating'].isin(['Medium','High'])), 'HousingReadyDistrict'] = 'Yes'
  
  final_data['JobAccessRating5'] = pd.qcut(final_data['JobAccess'],q=5,labels=['Very Low','Low','Medium','High','Very High'])
  final_data['HousingReadyDistrict2'] = 'No'
  final_data.loc[(final_data['SchoolEnrollmentStatus'] == 'Under enrolled') & (final_data['JobAccessRating'].isin(['Medium','High','Very High'])), 'HousingReadyDistrict2'] = 'Yes'
  
  # Store final metrics
  bucket = get_gcs_client().bucket(gcp_bucket)
  blob = bucket.blob('final_metrics')
  blob.upload_from_string(final_data) 
