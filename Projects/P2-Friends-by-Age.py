# Import necessary lib
from pyspark import SparkConf, SparkContext

# set up context
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# Data Cleaning def function
def parseline(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return(age, numFriends)

# Load Data
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")

# Extract Data using Map
rdd = lines.map(parseline)

# Count Up Sum of Friends and Number of Entries per Age
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1]+ y[1]))

averageByAge = totalsByAge.mapValues(lambda x : x[0]/x[1])

# Collect and Display the result


for result in averageByAge.collect():
    print(result)