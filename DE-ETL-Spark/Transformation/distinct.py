from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

data = sc.parallelize([1,2,3,"phuc",4,2,3,"dat","phuc"])
distinctData = data.distinct()
print(distinctData.collect())