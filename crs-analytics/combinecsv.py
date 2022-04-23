import os
import boto3
# import pandas as pd
# from datetime import datetime

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_KEY_ID']
AWS_REGION = "ap-southeast-1"
AWS_S3_BUCKET = "ebd-demo"

AWS_WRITE_OBJ_KEY = "prediction/"

s3_client = boto3.client('s3',
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                         region_name=AWS_REGION
                         )

# 1. defines path to excel files
path = "temp_results_location/"

# 2. creates list with excel files to merge based on name convention
file_list = [path + f for f in os.listdir(path) if f.endswith('c000.csv')]
# print(file_list)

for file in file_list:
    print(file)
    filename = file.split("/")[-1]
    response = s3_client.upload_file(file,AWS_S3_BUCKET, AWS_WRITE_OBJ_KEY+filename)
    # response = s3_client.put_object(
    #     Bucket=AWS_S3_BUCKET, Key=AWS_WRITE_OBJ_KEY+filename)
    # status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    # if status == 200:
    #     print(f"Successful S3 put_object response. Status - {status}")
    # else:
    #     print(f"Unsuccessful S3 put_object response. Status - {status}")



# 3. creates empty list to include the content of each file converted to pandas DF
# csv_list = []
#
#
# # 4. reads each (sorted) file in file_list, converts it to pandas DF and appends it to the csv_list
# for file in sorted(file_list):
#     csv_list.append(pd.read_csv(file).assign(File_Name=os.path.basename(file)))
#
# # print(csv_list)
#
# # 5. merges single pandas DFs into a single DF, index is refreshed
# csv_merged = pd.concat(csv_list, ignore_index=True)
# # drop fil name column
# csv_merged.drop('File_Name', axis=1, inplace=True)
#
# # 6. Single DF is saved to the path in Excel format, without index column
# csv_merged.to_csv(path + 'carpark-all-location-20220418T02.csv', index=False)
