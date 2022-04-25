import os
import io
import boto3
import pandas as pd
from datetime import datetime

start_time = datetime.now()

# get your credentials from environment variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_KEY_ID']
AWS_REGION = "ap-southeast-1"
AWS_S3_BUCKET = "ebd-demo"

# s3_client = boto3.client('s3',
#                          aws_access_key_id=AWS_ACCESS_KEY_ID,
#                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#                          region_name=AWS_REGION
#                          )

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    region_name=AWS_REGION)
my_bucket = s3.Bucket(AWS_S3_BUCKET)
files = my_bucket.objects.filter(Prefix='input/')
files = [obj.key for obj in sorted(files, key=lambda x: x.last_modified, reverse=True)][0:1]

print(files[0])

# get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
# objs = s3_client.list_objects_v2(Bucket=AWS_S3_BUCKET)['Contents']
# last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified)][0]
#
# print(last_added)
