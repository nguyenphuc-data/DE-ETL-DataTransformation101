from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

text = sc.parallelize(["phUC DEp Trai vai CHUng May a"])

data = text.flatMap(lambda x : x.split(" "))\
    .map(lambda x : x.lower())

removeData = sc.parallelize(["vai may"])\
    .flatMap(lambda x : x.split(" "))

afterData = data.subtract(removeData)
print(afterData.collect())