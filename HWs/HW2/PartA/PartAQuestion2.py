from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, current_timestamp
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import StructType, TimestampType

spark = SparkSession \
	.builder \
	.appName("PartAQuestion2") \
	.getOrCreate()



# Read all the csv files written atomically in a directory
# userA, userB, timestamp, interaction
userSchema = StructType()\
	.add("userA", "integer")\
	.add("userB", "integer")\
	.add("timestamp", TimestampType())\
	.add("interaction","string")

activity = spark \
	.readStream \
	.option("sep", ",") \
	.schema(userSchema) \
	.csv("higgs/stage/*.csv")


windowedData = activity.where("interaction = \"MT\"") \
			.select((current_timestamp().cast("bigint")-
				activity.timestamp.cast("bigint"))/(1000000) ).rdd[0]


# Generate running word count
wordCounts = activity \
			.select("userB") \
			.where("interaction = \"MT\"") \
			
			

# Start running the query that prints the running counts to the console
query = wordCounts \
	.writeStream.trigger(processingTime='10 seconds') \
	.format("parquet") \
	.option("path","higgs/stage/out") \
	.option("checkpointLocation","higgs/stage/checkpoint") \
	.start() \



query2 = wordCounts \
	.writeStream.trigger(processingTime='10 seconds') \
	.format("console") \
	.start()

query3 = windowedData \
	.writeStream.trigger(processingTime='10 seconds') \
	.format("console") \
	.start()

query.awaitTermination()
