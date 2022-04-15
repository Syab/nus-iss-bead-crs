from pyspark.sql.types import StructType, StructField,StringType,IntegerType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Statistics').getOrCreate()
sc = spark.sparkContext

data = [("James", "Sales", "SG", 70000, 34, 10000),
        ("Michael", "Sales", "SG", 66000, 56, 20000),
        ("Robert", "Sales", "MY", 61000, 30, 23000),
        ("Maria", "Finance", "MY", 60000, 24, 23000),
        ("Raman", "Finance", "USA", 79000, 40, 24000),
        ("Scott", "Finance", "USA", 63000, 36, 19000),
        ("Jen", "Finance", "UK", 89000, 53, 15000),
        ("Jeff", "Marketing", "UK", 70000, 25, 18000),
        ("Alice", "Marketing", "UK", 78000, 50, 21000),
        ("Ada", "IT", "SG", 83000, 35, 11000),
        ("Jackson", "IT", "MY", 71000, 30, 21000),
        ("Cooper", "IT", "UK", 91000, 40, 21000)]

# a. Create Dataframe
rdd = sc.parallelize(data)
df = spark.createDataFrame(rdd, ["employee_name", "department", "country", "salary", "age", "bonus"])

# b. show and print schema
df.show()
df.printSchema()

# c. Run groupBy() on “department” columns. Calculate aggregates like minimum, maximum,
#    average, total salary for each group using min(), max(), avg() and sum() aggregate functions
#    respectively.

df.groupBy("department")
# minimum
df.groupBy("department").min().show()
# maximum
df.groupBy("department").max().show()
# average
df.groupBy("department").mean().show()
# total
df.groupBy("department").sum().show()

# d. Run groupBy() on “country” columns. Calculate aggregates like minimum, maximum, average,
#    total salary for each group using min(), max(), avg() and sum() aggregate functions respectively.
df.groupBy("country")
# minimum
df.groupBy("country").min().show()
# maximum
df.groupBy("country").max().show()
# average
df.groupBy("country").mean().show()
# total
df.groupBy("country").sum().show()