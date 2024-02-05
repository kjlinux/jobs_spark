from pyspark import SparkContext

sc = SparkContext("local", "First App")

# Load the data
customer = sc.textFile("/home/master/customer.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda array: (int(array[0]), array[2]))

order = sc.textFile("/home/master/order.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda array: (int(array[0]), int(array[1])))

# Perform the join operation
joined = customer.join(order)

# Print the result
for i in joined.collect():
    print(i)

