from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("../Data/dataDE.txt")
flatmapRdd = fileRdd.flatMap(lambda x : x.split(" "))

for line in flatmapRdd.collect():
    print(line)