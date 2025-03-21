# %%
import os
import logging
import boto3

from aind_data_access_api.document_db import MetadataDbClient

logger = logging.getLogger(__name__)

PROJECT = "dynamic-foraging-analysis"
S3_RESULTS_ROOT = f"s3://aind-{PROJECT}-prod-o5171v"

s3_client = boto3.client("s3")


def upload_directory_to_s3(local_directory, s3_bucket_name, s3_relative_path):
    
    s3_bucket_name = s3_bucket_name.replace("s3://", "")
    
    # Walk the directory tree
    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            # Construct the full local file path
            local_path = os.path.join(root, filename)

            # Construct the S3 key (object name) by getting the relative path
            relative_path = os.path.relpath(local_path, start=local_directory)

            # Use the relative_path as the S3 object key to preserve the folder structure
            s3_key = s3_relative_path + '/' + relative_path

            # Upload file to S3
            s3_client.upload_file(local_path, s3_bucket_name, s3_key)
            
    logger.info(f"Uploaded {local_path} to s3://{s3_bucket_name}")
