# Import required Lib
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import LongType, StructType, StructField, IntegerType

# Create Spark Session
spark = SparkSession.builder.appName("PopularMovie").getOrCreate()

# Create Schema while reading the data
schema = StructType(\
                    [StructField("UserID", IntegerType(), True),\
                     StructField("MovieID", IntegerType(), True),\
                     StructField("Ratings", IntegerType(), True),\
                     StructField("Timestamp", LongType(), True)])
    
# Load Movie Data as dataframe
movie = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")
movie.printSchema()

# Sort all Movies by Popularity in one line
popularmovie = movie.groupBy("MovieID").count().orderBy(func.desc("count"))

# print top 10 rows
popularmovie.show(10)

# Stop the session
spark.stop()
