from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

spark = SparkSession.builder.appName("CustomerAmountSpent").getOrCreate()

# Create Schema when reading the Cutomer Orders csv file
schema = StructType(\
                    [StructField("CustomerID", IntegerType(), True),\
                     StructField("ProductID", IntegerType(), True),\
                     StructField("Amount_Spent", FloatType(), True)])

# Load Data
purchase = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
purchase.printSchema()

# Select only customerID and Amount_Spent
CustomerSpent = purchase.select("CustomerID", "Amount_Spent")


totalcustomerspent = CustomerSpent.groupBy("CustomerID").agg(func.round(func.sum("Amount_Spent"), 2).alias("Total_Spent"))

totalcustomerspentsorted = totalcustomerspent.sort("Total_Spent")

totalcustomerspentsorted.show(totalcustomerspentsorted.count())

spark.stop()