import random
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Phuc") \
    .master("local[*]") \
    .config("spark.executor.memory","4g") \
    .getOrCreate()

rdd = spark.sparkContext.parallelize(range(1,11)) \
    .map(lambda x : (x,random.randint(0,99) * x))
schema = ["key","value"]

df = spark.createDataFrame(rdd,schema).show()

