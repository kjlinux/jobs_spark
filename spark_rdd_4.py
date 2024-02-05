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

# Calculate the average total per customer
avgTotal = joined.mapValues(lambda x: (x[1], 1)) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda x: x[0] / x[1])

# Sort by customer name
sortedAvgTotal = avgTotal.sortBy(lambda x: x[1])

# Print the result
for i in sortedAvgTotal.collect():
    print(i)
