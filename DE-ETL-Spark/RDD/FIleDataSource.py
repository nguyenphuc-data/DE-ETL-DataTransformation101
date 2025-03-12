from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

fileRDdd = sc.textFile("../Data/dataDE.txt")
print(fileRDdd.collect())
print(f"Number of data: {fileRDdd.count()}")