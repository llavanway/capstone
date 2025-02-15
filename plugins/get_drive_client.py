import os
import json
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io
from pathlib import Path
import logging

def get_drive_client():
    """Initialize and return a Google Drive client."""
    creds_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not creds_json:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")
    
    creds_dict = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return build('drive', 'v3', credentials=credentials)
