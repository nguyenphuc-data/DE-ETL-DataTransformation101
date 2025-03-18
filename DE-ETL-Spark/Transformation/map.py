from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("../Data/dataDE.txt")
print(fileRdd.collect())

upperRdd = fileRdd.map(lambda x : x.upper())
for line in upperRdd.collect():
    if len(line) != 0:
        print(line)