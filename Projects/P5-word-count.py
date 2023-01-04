# Import required lib
from pyspark import SparkConf, SparkContext
import re

# Set up Context
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Load Data
input = sc.textFile("file:///SparkCourse/book.txt")

# Text Normalization
def normalization(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# Flatmap can create many new elements from each one
words = input.flatMap(normalization)

# Adds up all values for each unique key ! 
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# flip (word, count) = (count, word) And Sort by Count as count is our new key
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

# clean and print the data
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
