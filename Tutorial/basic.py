print("Start ...")
  
from pyspark.sql import SparkSession
spark = SparkSession \
       .builder \
       .master('yarn') \
       .appName("Python Spark SQL basic example 2") \
       .getOrCreate()

print("Spark Object is created")
print("Spark Version used is :" + spark.sparkContext.version)

data = [('Alice', 1)]
df=spark.createDataFrame(data)
print(df.count())


print("... End")