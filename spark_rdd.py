from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("CustomerOrderCount").setMaster("local")
sc = SparkContext(conf=conf)

customer_rdd = sc.textFile("file:/home/master/customer.txt")
order_rdd = sc.textFile("file:/home/master/order.txt")

customer_month_count = customer_rdd.map(lambda line: (line.split(",")[1].split("/")[1], 1))

customer_month_count = customer_month_count.reduceByKey(lambda x, y: x + y)

results = customer_month_count.collect()
for result in results:
    print(result)
