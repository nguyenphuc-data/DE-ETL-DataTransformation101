from pyspark.sql import Row,SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, FloatType, BooleanType, TimestampType, ArrayType, MapType, DateType, DecimalType
from datetime import datetime
from decimal import Decimal
spark = SparkSession.builder \
    .appName("Phuc") \
    .master("local[*]") \
    .config("spark.executor.memory","4g") \
    .getOrCreate()

# data = spark.sparkContext.parallelize([
#     Row(1,"phuc",20),
#     Row(2,"hieu",24),
#     Row(None,None,None),
#     Row(3,"dat",30)
# ])
# schema = StructType([
#     StructField("id",LongType(),True),
#     StructField("name",StringType(),True),
#     StructField("age",LongType(),True)
# ])
#
# df = spark.createDataFrame(data,schema).show()

data = [
    Row(
        id = 1,
        name = "Nguyen Huu Phuc",
        age = 20,
        salary = 10000.0,
        bonus = 5000.75,
        is_active = True,
        scores = [1,8,9],
        attributes = {"dept":"Engineer","role":"Data Engineer"},
        hire_date = datetime.strptime("2024-1-14","%Y-%m-%d").date(),
        last_login = datetime.strptime("2025-3-14 12:12:12","%Y-%m-%d %H:%M:%S"),
        tax_rate = Decimal("234.56")
    ),
    Row(
        id = 2,
        name = "Tran Quang Anh",
        age = 20,
        salary = 1500.0,
        bonus = 500.75,
        is_active = False,
        scores = [1,5,9],
        attributes = {"dept":"Engineer","role":"Data Engineer"},
        hire_date = datetime.strptime("2023-1-14","%Y-%m-%d").date(),
        last_login = datetime.strptime("2023-3-14 12:12:12","%Y-%m-%d %H:%M:%S"),
        tax_rate = Decimal("214.45")
    )
]
schema = StructType([
    StructField("id",LongType(),False),
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("salary",FloatType(),True),
    StructField("bonus",FloatType(),True),
    StructField("is_active",BooleanType(),True),
    StructField("scores",ArrayType(IntegerType()),True),
    StructField("attributes",MapType(StringType(),StringType()),True),
    StructField("hire_date",DateType(),True),
    StructField("last_login",TimestampType(),True),
    StructField("tax_rate",DecimalType(5,2),True)
])

df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()
"""
PYSPARK SQL TYPE
StringType: chuoi ky tu
LongType: so nguyen 64 bit
IntegerType: so nguyen 32 bit
FloatType: so thap phan 32 bit
DoubleType: so thap phan 64 bit
BooleanType: true/false
TimestampType: ngay va gio
DateType: nam/thang/ngay
DecimalType(precision,scale)
ByteType: so nguyen 8 bit
ShortType: so nguye 16 bit

================
ADVANCE
StructType: bieu dien mot cau truc
StructField(name,datatype,nullable): bieu dien 1 truong trong StructType
ArrayType(elementType): bieu dien cac mang duoc chi dinh
    - elementType": kieu du lieu cua cac phan tu trong mang
MapType(keyType, valueType): bieu dien cap khoa key-value
    - keyType: kieu du lieu cua key
    - valueTYpe: kieu du lieu cua gia tri value
"""