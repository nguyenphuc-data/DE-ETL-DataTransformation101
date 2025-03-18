from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

data = sc.parallelize([11,2,3,4,5,6,7,8,9,10],2)
print(data.take(7))