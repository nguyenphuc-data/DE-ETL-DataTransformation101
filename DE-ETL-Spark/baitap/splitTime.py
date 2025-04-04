from pyspark.sql import Row,SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, FloatType, BooleanType, TimestampType, ArrayType, MapType, DateType, DecimalType
from datetime import datetime
from decimal import Decimal
from pyspark.sql.functions import split
import re
spark = SparkSession.builder \
    .appName("Phuc") \
    .master("local[*]") \
    .config("spark.executor.memory","4g") \
    .getOrCreate()

data = [["11//02/2025"],
        ["27/11-2001"],
        ["28.12-2005"],
        ["14~9*2002"],
        ["-31:03{}1995"]]

df = spark.createDataFrame(data,["date"])
df_new = []
for x in df.collect():
    new_date = ""
    for y in x.date:
        if y.isdigit():
            new_date += y
        else:
            new_date += " "
    cleaned_date = re.sub(r"\s+", " ", new_date).strip()
    df_new.append((x.date, cleaned_date))

data_new = spark.createDataFrame(df_new, ["date", "clean_date"])

data_after = data_new.withColumn("split", split(data_new["clean_date"], " "))

data_after = data_after.withColumn("day", data_after["split"][0])\
                       .withColumn("month", data_after["split"][1])\
                       .withColumn("year", data_after["split"][2])\
                       .drop("split", "clean_date")

data_after.show(truncate=False)

