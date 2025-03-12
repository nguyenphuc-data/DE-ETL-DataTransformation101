# join trong sql nghia la gi: noi 2 bang co cot chung
# giai quyet bai toan dong bo du lieu
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

data1 = sc.parallelize([(110,50.12), (127,90.5), (120,211.0)])
data2 = sc.parallelize([(110,"phuc"), (127,"quanh"), (120,"dat")])

dataNew = data1.join(data2).sortByKey()
for result in dataNew.collect():
    print(result)