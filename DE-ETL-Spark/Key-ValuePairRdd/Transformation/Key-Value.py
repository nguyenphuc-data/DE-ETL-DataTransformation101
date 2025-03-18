from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Phuc").setMaster("local[*]").set("spark.excutor.memory","4g")

sc = SparkContext(conf=conf)

# rdd = sc.parallelize(["phuc dep trai qua di bay oi"])
# rdd2 = rdd.flatMap(lambda x : x.split(" "))
#
# pairRdd = rdd2.map(lambda x : (len(x),x))
# for pair in pairRdd.collect():
#     print(pair)
#
# groupRdd = pairRdd.reduceByKey(lambda x,y: x + y)
# print(groupRdd.collect())
# for key,value in groupRdd.collect():
#     print(key,list(value))

# Tinh diem trung binh
# scores = [("Math", 85), ("English", 90), ("Math", 95), ("English", 80), ("Science", 75), ("Math", 90)]
# data = sc.parallelize(scores)
#
# rdd_count = data.map(lambda x : (x[0],(x[1],1)))
# print(rdd_count.collect())
#
# rdd_reduce = rdd_count.reduceByKey(lambda x,y : ((x[0]+y[0]),(x[1]+y[1])))
# print(rdd_reduce.collect())
#
# rdd_average = rdd_reduce.map(lambda x : (x[0],x[1][0] / x[1][1]))
# print(rdd_average.collect())

# Phan tich giao dich tai chinh

transactions = [
    ("2025-03-01", "AAPL", 150, 155),  # Lợi nhuận: 155 - 150 = 5
    ("2025-03-01", "GOOG", 1200, 1210), # Lợi nhuận: 10
    ("2025-03-01", "AAPL", 152, 158),   # Lợi nhuận: 6
    ("2025-03-02", "AAPL", 160, 165),   # Lợi nhuận: 5
    ("2025-03-02", "GOOG", 1210, 1220), # Lợi nhuận: 10
    ("2025-03-02", "MSFT", 300, 310),   # Lợi nhuận: 10
    ("2025-03-01", "MSFT", 295, 305)    # Lợi nhuận: 10
]

data = sc.parallelize(transactions)
# Tinh loi nhuan
data_profit = data.map(lambda x : ((x[0],x[1]),(x[3]-x[2],1)))
print(data_profit.collect())
# Gop tong loi nhuan moi ma co phieu theo tung ngay
data_sum_profit = data_profit.reduceByKey(lambda x,y : ((x[0]+y[0]),(x[1]+y[1])))
print(data_sum_profit.collect())
# In ra
for (date,stock),(total_profit,count) in data_sum_profit.collect():
    print(f"Date : {date}, Stock : {stock}, Total_profit : {total_profit}, Count : {count}")