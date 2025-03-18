# Map di den tung hang
# import library
from pyspark import SparkContext

# create sparkContext
sc = SparkContext("local","Phuc")

#create object
data = [
    {"id":1, "name":"phuc"},
    {"id":2, "name":"dat"},
    {"id":3, "name":"quanh"},
]

# create rdd from data
rdd = sc.parallelize(data)
print(rdd.collect())
print(f"NUmber of data: {rdd.count()}")
print(f"first value of data: {rdd.first()}")