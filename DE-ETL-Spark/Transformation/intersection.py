from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([1,2,3,4,5])
rdd2 = sc.parallelize([4,5,6,7,8])

rdd3 = rdd1.intersection(rdd2)
print(rdd3.collect())