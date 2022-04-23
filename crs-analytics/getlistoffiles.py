from google.cloud import storage
from io import StringIO
import csv
import os
import boto3
import pandas as pd
from datetime import datetime

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_KEY_ID']
AWS_REGION = "ap-southeast-1"
AWS_S3_BUCKET = "ebd-demo"
AWS_WRITE_OBJ_KEY = "testprediction/"
GCP_CS_BUCKET = "ebd-crs-analytics"
GCP_CS_PREFIX = "temp_results/"
GCP_CS_DELIMITER = ".csv"

storage_client = storage.Client()
s3_client = boto3.client('s3',
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                         region_name=AWS_REGION
                         )
# Note: Client.list_blobs requires at least package version 1.17.0.
blobs = storage_client.list_blobs('ebd-crs-analytics', prefix='temp_results/', delimiter='.csv')

print("Blobs:")
for blob in blobs:
    print(blob.name)

# if delimiter:
print("Prefixes:")
print(list(blobs.prefixes))
for prefix in blobs.prefixes:
    print(prefix.split('/')[-1])

mybucket = storage_client.get_bucket(GCP_CS_BUCKET)
myblob = mybucket.get_blob('temp_results/part-00000-d694f861-f016-4e45-bd6f-69d43c10a7c3-c000.csv')
# downloaded_file = myblob.download_as_text(encoding="utf-8")
# df = pd.read_csv(downloaded_file)
# print(df.head(10))

# for file in list(blobs.prefixes):
#     print(file)
#     filename = file.split("/")[-1]
    # response = s3_client.upload_file(filename, AWS_S3_BUCKET, AWS_WRITE_OBJ_KEY + filename)