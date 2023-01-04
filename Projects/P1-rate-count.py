# Import Necessary Lib
from pyspark import SparkConf, SparkContext
import collections

# Set up Context
conf = SparkConf().setMaster("local").setAppName("ratingsHistogram")
sc = SparkContext(conf=conf)

# Load Data
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data") 

# Extract Data by using Map
ratings = lines.map(lambda x : x.split()[2])

# Count by Value
result = ratings.countByValue()

# Sort and Display the result
sortedresults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedresults.items():
    print("%s %i" % (key, value))
