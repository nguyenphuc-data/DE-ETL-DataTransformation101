import time

from numba.core.imputils import iterator_impl
from pyspark import SparkContext, SparkConf
from random import Random

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

data = ["Dat","Golden","Heu","Sami"]

rdd = sc.parallelize(data)

def numsPartition(iterator):
    #  create 1 number for map Partition data
    rand = Random(int(time.time()*1000) + Random().randint(0,1000))
    return [f"{item}:{rand.randint(0,1000)}" for item in iterator]

result = rdd.mapPartitions(numsPartition)
print(result.collect())