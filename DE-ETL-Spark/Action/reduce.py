from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

data = sc.parallelize([1,2,3,4,5,6,7,8,9,10],3)
print(data.glom().collect())
def sum(v1: int,v2: int) -> int:
    return v1 + v2
print(data.reduce(sum))
