from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

configurations = spark.sparkContext.getConf().getAll()
for item in configurations: print(item)