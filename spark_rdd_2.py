from pyspark import SparkContext

sc = SparkContext("local", "First App")

customer = sc.textFile("/home/master/customer.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda array: (int(array[0]), array[1], array[2]))

sortedCustomer = customer.sortBy(lambda x: x[2])

for i in sortedCustomer.collect():
    print(i)

