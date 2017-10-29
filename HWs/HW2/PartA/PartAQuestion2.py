from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
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
	.readStream.trigger(processingTime='10 minutes')  \
	.option("sep", ",") \
	.schema(userSchema) \
	.csv("higgs/stage/*.csv")


# Generate running word count
wordCounts = activity \
			.select("userB") \
			.where("interaction = \"MT\"")
			

# Start running the query that prints the running counts to the console
query = wordCounts \
	.writeStream.trigger(processingTime='10 minutes') \
	.format("parquet") \
	.option("path","higgs/stage/out") \
	.option("checkpointLocation","higgs/stage/checkpoint") \
	.start() \



query = wordCounts \
	.writeStream \
	.format("console") \
	.start()

query.awaitTermination()