
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




###保存再运行！！！！！！！！

# # Define the input TSV file path and output GZ file path
input_file_path = 'download/20230323/20230213_meta.tsv'
output_file_path = 'download/20230323/20230213_meta.tsv.gz'

# Create an empty GZ file
with gzip.open(output_file_path, 'w') as output_file:
    pass

# with open(input_file_path,'w') as output_file:
#     output_file.write('Column 1\tColumn 2\tColumn 3\n')
#     # Write the data rows to the output TSV file
#     output_file.write('Data 1\tData 2\tData 3\n')
#     output_file.write('Data 4\tData 5\tData 6\n')

# # Open the input TSV file in binary mode
with open(input_file_path, 'rb') as input_file:

    # Open the output GZ file in binary mode
    with gzip.open(output_file_path, 'wb') as output_file:

        # Read the contents of the input TSV file and write them to the output GZ file
        output_file.write(input_file.read())

