from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from prophet import Prophet
import pandas as pd
from datetime import datetime

start_time = datetime.now()

spark = SparkSession.builder.appName('testML').master("local[*]").getOrCreate()
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
    return result_pd[
        ['ds', 'car_park_no', 'latitude', 'longitude', 'total_lots', 'y', 'yhat', 'yhat_upper', 'yhat_lower']]


# load data into panda dataframe and convert ds column to date time
path = './carpark-all-location-20220420T16.csv'
# path = 'gs://ebd-crs-analytics/carpark-all-location-20220421T09.csv'

df = pd.read_csv(path)
df.drop_duplicates(inplace=True)
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
# carparkdf.show(10)
# # Apply time series forecasting
results = (carparkdf.groupby('car_park_no').apply(forecast_result).withColumn('training_date', current_date()))
results.cache()
results.show()

# Save results to csv
results.write.option("header", "true").mode('overwrite').format('csv').save('./testdata_all_location')

# outpath = "gs://ebd-crs-analytics/temp_results"
# results.write.option("header", "true").mode('overwrite').format('csv').save(outpath)

print('Duration: {}'.format(datetime.now() - start_time))
