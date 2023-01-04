# Import necessary Lib
from pyspark import SparkConf, SparkContext

# set up context
conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

# Data Cleaning using def function

def parsedline(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) + 32.0
    return (stationID, entryType, temperature)

# Load Data
lines = sc.textFile("file:///SparkCourse/1800.csv")

# Extract Data using map
parsedlines = lines.map(parsedline)

# Filter out all But Tmax entries
maxtemps = parsedlines.filter(lambda x:"TMAX" in x[1])

# Create (stationID, Temperature) key/value pairs
stationtemps = maxtemps.map(lambda x: (x[0], x[2]))

#find maximum temperature by stationID
mintemps = stationtemps.reduceByKey(lambda x, y: max(x,y))

for result in mintemps.collect():
    print(result[0] + "\t{:.2f}F".format(result[1]))