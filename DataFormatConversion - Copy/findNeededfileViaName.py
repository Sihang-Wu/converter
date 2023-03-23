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

DownloadFolder = 'download/20230323/'
# Define the path to the folder
folder_path = DownloadFolder
# Open the folder and read its contents
for root, dirs, files in os.walk(folder_path):
    # Check if any of the subfolders contain 'meta' in their name
    print(root)
    print(files)
    for file_name in files:
        if 'meta' in file_name:
            filename_meta = os.path.join(root, file_name)
            print(f'Found folder with meta: {os.path.join(root, file_name)}')

            print(f'Found folder with meta: {file_name}')
