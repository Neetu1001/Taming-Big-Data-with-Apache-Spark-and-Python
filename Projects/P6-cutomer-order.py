# Import required Lib
from pyspark import SparkConf, SparkContext

# Set up Context
conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf = conf)

# data cleaning def function
def parsedline(line):
    fields = line.split(',')
    return(int(fields[0]),float(fields[2]))

# Load Data
input = sc.textFile("file:///SparkCourse/customer-orders.csv")
mappedinput = input.map(parsedline)
totalByCustomer = mappedinput.reduceByKey(lambda x, y: x + y)

#Changed for Python 3 compatibility:
#flipped = totalByCustomer.map(lambda (x,y):(y,x))
flipped = totalByCustomer.map(lambda x: (x[1],x[0]))

# sort by Customer spending
sortedcustomerspent = flipped.sortByKey()

# Collect and display the result
for spent in sortedcustomerspent.collect():
    print(spent)