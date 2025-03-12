from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

rdd = sc.parallelize(["phuc dep trai qua di bay oi"])
rdd2 = rdd.flatMap(lambda x : x.split(" "))

pairRdd = rdd2.map(lambda x : (len(x),x))
print(pairRdd.collect())

groupByKeyRdd = pairRdd.groupByKey()
for keys in groupByKeyRdd.collect():
    print(keys)

for key, values in groupByKeyRdd.collect():
    print(key, list(values))