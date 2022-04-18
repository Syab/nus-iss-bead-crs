from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from prophet import Prophet
import os
import io
import boto3
import pandas as pd
from datetime import datetime

start_time = datetime.now()

# get your credentials from environment variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET']
AWS_REGION = "ap-southeast-1"
AWS_S3_BUCKET = "ebd-demo"

AWS_WRITE_OBJ_KEY = "testprediction/"

spark = SparkSession.builder.appName('sparks3dataprocML').master("local[*]").getOrCreate()
result_schema = StructType([
    StructField('ds', TimestampType()),
    StructField('car_park_no', StringType()),
    StructField('latitude', StringType()),
    StructField('longitude', StringType()),
    StructField('total_lots', StringType()),
    StructField('y', IntegerType()),
    StructField('yhat', IntegerType()),
    StructField('yhat_upper', IntegerType()),
    StructField('yhat_lower', IntegerType()),
])

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


def get_latest_file():
    s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=AWS_REGION)
    my_bucket = s3.Bucket(AWS_S3_BUCKET)
    files = my_bucket.objects.filter(Prefix='input/')
    files = [obj.key for obj in sorted(files, key=lambda x: x.last_modified, reverse=True)][0:1]
    print(files[0])
    return files[0]


@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP)
def forecast_result(carpark_pd):
    model = Prophet(interval_width=0.95, growth='linear', daily_seasonality=True)
    model.fit(carpark_pd)
    future_pd = model.make_future_dataframe(periods=48, freq='30min', include_history=True)
    forecast_pd = model.predict(future_pd)
    # convert negative values to zero
    num = forecast_pd._get_numeric_data()
    num[num < 0] = 0
    f_pd = forecast_pd[['ds', 'yhat', 'yhat_upper', 'yhat_lower']].set_index('ds')
    cp_pd = carpark_pd[['ds', 'car_park_no', 'y', 'latitude', 'longitude', 'total_lots']].set_index('ds')
    result_pd = f_pd.join(cp_pd, how='left')
    result_pd.reset_index(level=0, inplace=True)
    result_pd['car_park_no'] = carpark_pd['car_park_no'].iloc[0]
    result_pd['latitude'] = carpark_pd['latitude'].iloc[0]
    result_pd['longitude'] = carpark_pd['longitude'].iloc[0]
    return result_pd[['ds', 'car_park_no', 'latitude', 'longitude', 'total_lots','y', 'yhat', 'yhat_upper', 'yhat_lower']]


s3_client = boto3.client('s3',
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                         region_name=AWS_REGION
                         )

AWS_READ_OBJ_KEY = get_latest_file()
response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=AWS_READ_OBJ_KEY)

status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    print(f"Successful S3 get_object response. Status - {status}")
    df = pd.read_csv(response.get("Body"))
    df['ds'] = pd.to_datetime(df['ds'])
    df['latitude'] = df['latitude'].astype(str)
    df['longitude'] = df['longitude'].astype(str)
    df['total_lots'] = df['total_lots'].astype(str)
    # Convert to Spark dataframe
    sdf = spark.createDataFrame(df)
    sdf.printSchema()
    sdf.show(10)
    sdf.count()

    # Repartition dataframe by carpark no
    carparkdf = sdf.repartition(spark.sparkContext.defaultParallelism, ['car_park_no']).cache()

    # Apply time series forecasting
    results = (carparkdf.groupby('car_park_no').apply(forecast_result).withColumn('training_date', current_date()))
    results.cache()
    results.show()
    results.write.option("header", "true").mode('overwrite').format('csv').save('./temp_results_location')

    # Convert Back to Pandas and output to file
    # print("converting to pandas again...")
    # result_df = results.select("*").toPandas()
    #
    # response = s3_client.put_object(
    #     Bucket=AWS_S3_BUCKET, Key=AWS_WRITE_OBJ_KEY)
    #
    # status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    # if status == 200:
    #     print(f"Successful S3 put_object response. Status - {status}")
    # else:
    #     print(f"Unsuccessful S3 put_object response. Status - {status}")

    # with io.StringIO() as csv_buffer:
    #     print("Write to file and push to S3")
    #     result_df.to_csv(csv_buffer, index=False)
    #     response = s3_client.put_object(
    #         Bucket=AWS_S3_BUCKET, Key=AWS_WRITE_OBJ_KEY, Body=csv_buffer.getvalue()
    #     )
    #
    #     status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    #     if status == 200:
    #         print(f"Successful S3 put_object response. Status - {status}")
    #     else:
    #         print(f"Unsuccessful S3 put_object response. Status - {status}")

else:
    print(f"Unsuccessful S3 get_object response. Status - {status}")

print('Duration: {}'.format(datetime.now() - start_time))
