from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StringType, StructField, IntegerType, FloatType, StructType

spark = SparkSession.builder.appName("MinimumTemperature").getOrCreate()

schema = StructType(\
                    [StructField("StationID", StringType(), True),\
                     StructField("Date", IntegerType(), True), \
                     StructField("Measure_type", StringType(), True),\
                     StructField("Temperature", FloatType(), True)])

# Read the file as DataFrame
temp = spark.read.schema(schema).csv("file:///SparkCourse/1800.csv")
temp.printSchema()

# Filter out all but TMIN entries
minitemp = temp.filter(temp.Measure_type == "TMIN")

# Select only StationID and Temperature
stationtemp = minitemp.select("StationID", "Temperature")

# Aggregate to find minimum temerature for every station
ministationtemp = stationtemp.groupBy("StationID").min("Temperature")
ministationtemp.show()

# Convert Temperature to Farenheit and sort the dataset
ministationtempF = ministationtemp.withColumn("Temperature", func.round(func.col("min(Temperature)") * 0.1 * (9.0/5.0) + 32.0, 2))\
                                              .select("StationID", "Temperature").sort("Temperature")

# Collect, format and print the resullt                                             
result = ministationtempF.collect()

for r in result:
    print(r[0] + "\t{:.2f}F".format(r[1]))

spark.stop()