from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

data = sc.parallelize([("phuc-debt",5.0), ("dat-debt",6.2),
                       ("quanh-debt",10.5), ("dat-debt",7.8), ("quanh-debt",9.6)]).countByKey()

print(dict(data))