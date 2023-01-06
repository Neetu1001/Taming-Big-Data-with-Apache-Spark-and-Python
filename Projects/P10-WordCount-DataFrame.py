# Import required lib
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of the book into a dataframe
lines = spark.read.text("file:///SparkCourse/book.txt")

# Split using a regular expression that extracts words
words = lines.select(func.explode(func.split(lines.value, "\\W+")).alias("Word"))
words.filter(words.Word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.Word).alias("Word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("Word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show full results
wordCountsSorted.show(wordCountsSorted.count())