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


file = 'download/20230323/20230213_meta.tsv.gz'

if file.endswith('.gz'):
    # Open the compressed file in binary mode
    with gzip.open(file, 'rb') as f_in:
        # Open the output file in binary mode
        with open(file[:-3], 'wb') as f_out:
            # Copy the contents of the compressed file to the output file
            shutil.copyfileobj(f_in, f_out)
    print(f'Extracted {file} to {file[:-3]}')