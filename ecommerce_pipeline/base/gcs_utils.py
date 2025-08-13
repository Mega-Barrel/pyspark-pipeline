# -*- coding: utf-8 -*-

import os
import io
from pathlib import Path
from typing import Optional, List

import pandas as pd
from google.api_core import exceptions
from google.cloud import storage
from google.oauth2 import service_account

class GcsStorageManager:
    """A helper class for interacting with Google Cloud Storage.

    This class provides a simplified interface for common GCS operations.
    It handles client authentication and provides methods for managing
    buckets and the files (blobs) within them.

    Attributes:
        storage_client (storage.Client): The authenticated GCS client.
    """

    def __init__(self, credentials_path: Optional[str] = None):
        """Initializes the GcsStorageManager.

        Args:
            credentials_path (Optional[str], optional): Path to the service account
                JSON file. If None, authentication is attempted via environment
                variables (e.g., GOOGLE_APPLICATION_CREDENTIALS).

        Raises:
            FileNotFoundError: If a credentials_path is provided but the file
                does not exist.
        """
        if credentials_path:
            if not Path(credentials_path).exists():
                raise FileNotFoundError(f"Credentials file not found at: {credentials_path}")
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.storage_client = storage.Client(credentials=credentials)
        else:
            # This will use default credentials from the environment
            self.storage_client = storage.Client()
        print("GCS Storage Manager initialized successfully.")

    def create_bucket(self, bucket_name: str, location: str = "asia-south1") -> Optional[storage.Bucket]:
        try:
            print(f"Attempting to create or get bucket: {bucket_name}")
            bucket = self.storage_client.bucket(bucket_name)
            if not bucket.exists():
                bucket = self.storage_client.create_bucket(bucket, location=location)
                print(f"Bucket '{bucket_name}' created successfully in {location}.")
            else:
                print(f"Bucket '{bucket_name}' already exists.")
            return bucket
        except exceptions.Conflict as e:
            print(f"Error: Bucket name '{bucket_name}' is likely already taken. {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred while creating bucket: {e}")
            return None

    def upload_file(self, bucket_name: str, local_file_path: str, destination_blob_name: str) -> None:
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_file_path)
            print(f"File '{local_file_path}' uploaded to 'gs://{bucket_name}/{destination_blob_name}'.")
        except FileNotFoundError:
            print(f"Error: Local file not found at '{local_file_path}'.")
        except exceptions.NotFound:
            print(f"Error: Bucket '{bucket_name}' not found.")
        except Exception as e:
            print(f"An unexpected error occurred during upload: {e}")

    def list_files(self, bucket_name: str, prefix: Optional[str] = None) -> List[storage.Blob]:
        print(f"Listing files in bucket '{bucket_name}' with prefix '{prefix or ''}'...")
        try:
            blobs = self.storage_client.list_blobs(bucket_name, prefix=prefix)
            return list(blobs)
        except exceptions.NotFound:
            print(f"Error: Bucket '{bucket_name}' not found.")
            return []
        except Exception as e:
            print(f"An unexpected error occurred while listing files: {e}")
            return []

    def read_csv_from_blob(self, bucket_name: str, blob_name: str, pd) -> Optional[pd.DataFrame]:
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            if not blob.exists():
                print(f"Error: File '{blob_name}' not found in bucket '{bucket_name}'.")
                return None

            # Download the content of the blob as a string
            csv_string = blob.download_as_string().decode('utf-8')

            # Use StringIO to treat the string as a file for pandas
            df = pd.read_csv(io.StringIO(csv_string))
            print(f"Successfully read 'gs://{bucket_name}/{blob_name}' into a DataFrame.")
            return df

        except Exception as e:
            print(f"An unexpected error occurred while reading the CSV: {e}")
            return None

    def delete_file(self, bucket_name: str, blob_name: str) -> None:
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
            print(f"File 'gs://{bucket_name}/{blob_name}' deleted successfully.")
        except exceptions.NotFound:
            print(f"Error: File '{blob_name}' not found in bucket '{bucket_name}'.")
        except Exception as e:
            print(f"An unexpected error occurred during file deletion: {e}")

    def delete_bucket(self, bucket_name: str) -> None:
        try:
            bucket = self.storage_client.get_bucket(bucket_name)
            bucket.delete()
            print(f"Bucket '{bucket_name}' deleted successfully.")
        except exceptions.NotFound:
            print(f"Error: Bucket '{bucket_name}' not found.")
        except exceptions.Conflict as e:
            print(f"Error: Bucket '{bucket_name}' is not empty. {e}")
        except Exception as e:
            print(f"An unexpected error occurred during bucket deletion: {e}")
