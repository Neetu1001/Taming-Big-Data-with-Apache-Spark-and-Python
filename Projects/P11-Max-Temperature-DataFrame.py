from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

spark = SparkSession.builder.appName("MaximumTemperature").getOrCreate()

schema = StructType(\
                    [StructField("stationID", StringType(), True),\
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True),\
                     StructField("temperature", FloatType(), True)])

temp = spark.read.schema(schema).csv("file:///SparkCourse/1800.csv")
temp.printSchema()

maxtemp = temp.filter(temp.measure_type == "TMAX")

stationtemp = maxtemp.select("stationID", "temperature")

maxtempbystation = stationtemp.groupBy("stationID").max("temperature")
maxtempbystation.show()

maxtempbystationF = maxtempbystation.withColumn("temperature", func.round(func.col("max(temperature)") * 0.1 * (9.0/5.0)+32.0,2))\
    .select("stationID", "temperature").sort("temperature")

for result in maxtempbystationF.collect():
    print(result[0] + "\t{:.2f}F".format(result[1]))