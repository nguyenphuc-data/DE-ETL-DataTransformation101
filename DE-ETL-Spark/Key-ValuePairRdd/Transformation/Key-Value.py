from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

rdd = sc.parallelize(["phuc dep trai qua di bay oi"])
rdd2 = rdd.flatMap(lambda x : x.split(" "))

pairRdd = rdd2.map(lambda x : (len(x),x))
for pair in pairRdd.collect():
    print(pair)