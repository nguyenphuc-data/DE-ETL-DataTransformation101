from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

number = [1,2,3,4,5,6,7,8,9,10]
rdd = sc.parallelize(number)

# Using transformation to create rdd
squareRdd = rdd.map(lambda i: i*i) #hàm ẩn danh, không cần định nghĩa
print(squareRdd.collect())

filterRdd = rdd.filter(lambda i : i>4)
print(filterRdd.collect())

flatmapRdd = rdd.flatMap(lambda i : [i,i*2])
print(flatmapRdd.collect())
