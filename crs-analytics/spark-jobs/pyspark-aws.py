from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime

spark = SparkSession.builder.appName('crs-analytics').getOrCreate()
sc = spark.sparkContext

# Convert to Spark dataframe
sdf = spark.read.csv("gs://ebd-crs-analytics/carpark-all-location-20220421T09.csv", header=True,inferSchema=True)
sdf.printSchema()
sdf.write.option("header", "true").mode('overwrite').format('csv').save('gs://ebd-crs-analytics/temp_results')
sdf.show(20)
