import os
import boto3
from botocore.exceptions import ClientError
import requests
import datetime
import time
import sqlite3
import hmac
import hashlib
import pandas as pd
import gzip
import shutil




date = datetime.date.today().strftime('%Y%m%d')
DownloadFolder = f'download/{date}/'
# Set up an region
region = "ap-southeast-1"
# # Define the name of the bucket and the object key of the folder
bucket_name = "hw51rddmp"
folder_key = f'1088513091236366080/converted/'
# # Set up an S3 client
s3 = boto3.client('s3',
                  aws_access_key_id="AKIA56GUWCT35UMJOYVT",
                  aws_secret_access_key="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",
                  region_name = region)


# Create a local folder to save the downloaded files
local_folder = DownloadFolder
os.makedirs(local_folder, exist_ok=True)

# List all objects in the folder
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_key)
print(response)

# {'ResponseMetadata': {'RequestId': 'VKSPSM4JMS7QYTRJ', 'HostId': 'c8QcOJFMuFaOVWj+S1ynmSZJfwT53RPGZsj
# DbBtuZUHl0C6wj7REaPmko8v4t4kiQFJa3BaCXu5d7S+dvPOggg==', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz
# -id-2': 'c8QcOJFMuFaOVWj+S1ynmSZJfwT53RPGZsjDbBtuZUHl0C6wj7REaPmko8v4t4kiQFJa3BaCXu5d7S+dvPOggg==', '
# x-amz-request-id': 'VKSPSM4JMS7QYTRJ', 'date': 'Thu, 23 Mar 2023 10:14:31 GMT', 'x-amz-bucket-regio
# n': 'ap-southeast-1', 'content-type': 'application/xml', 'transfer-encoding': 'chunked', 'server': '
# AmazonS3'}, 'RetryAttempts': 0}, 'IsTruncated': False, 
# 'Contents': [{'Key': '1088513091236366080/converted/--204605--add--DataNexx_DemoFacts_Gender_Age_Female_45_plus--1.2--.txt', 'LastModified': dateti
# me.datetime(2023, 3, 20, 11, 6, 29, tzinfo=tzutc()), 'ETag': '"a2baf9cfd800e4d7b7baf1d7ffa5d521"', 'Si
# ze': 480, 'StorageClass': 'STANDARD'}, 
# {'Key': '1088513091236366080/converted/--204605--del--DataNexx_DemoFacts_Gender_Age_Female_45_plus--1.2--.txt', 'LastModified': datetime.datetime(2023, 3, 20, 11, 
# 6, 30, tzinfo=tzutc()), 'ETag': '"cc8ac4a5a7ca9e83e968df03e18e23c7"', 'Size': 40, 'StorageClass': 'S
# TANDARD'}, 
# {'Key': '1088513091236366080/converted/--204689--add--DataNexx_Persona_GamblerLifestyle_ADEX--2.5--.txt', 'LastModified': datetime.datetime(2023, 3, 20, 11, 6, 31, tzinfo=tzutc()), 'ETag': '"
# c5c73353c3771f5d8ae613737d33c4f9"', 'Size': 280, 'StorageClass': 'STANDARD'}, 


for obj in response['Contents']:
    object_key = obj['Key']
    local_file = os.path.join(local_folder, os.path.basename(object_key))
    s3.download_file(bucket_name, object_key, local_file)
    print(f'Downloaded {object_key} to {local_file}')

    # Check if the downloaded file is a .gz file
    if local_file.endswith('.gz'):
        # Open the compressed file in binary mode
        with gzip.open(local_file, 'rb') as f_in:
            # Open the output file in binary mode
            with open(local_file[:-3], 'wb') as f_out:
                # Copy the contents of the compressed file to the output file
                shutil.copyfileobj(f_in, f_out)
        print(f'Extracted {local_file} to {local_file[:-3]}')







