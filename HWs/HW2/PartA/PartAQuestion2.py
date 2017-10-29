from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import StructType, TimestampType

spark = SparkSession \
	.builder \
	.appName("PartAQuestion2") \
	.getOrCreate()


# Create DataFrame representing the stream of input lines 
# from connection to localhost:9999
# lines = spark \
# 	.readStream \
# 	.format("socket") \
# 	.option("host", "localhost") \
# 	.option("port", 9999) \
# 	.load()

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
	.csv("higgs/stage/*.csv")  # Equivalent to format("csv").load("/path/to/directory")


def secondsBetween(col1, col2):
	col2.cast("timestamp").cast("bigint") - col1.cast("timestamp").cast("bigint")

def minutesBetween(col1, col2):
	(secondsBetween(col1, col2) / 60).cast("bigint")

# Generate running word count
wordCounts = activity \
			.select("userB") \
			.where("interaction = \"MT\"") \
			.where(	minutesBetween( $"current_timestamp()" , $"timestamp")<10)
				


# Start running the query that prints the running counts to the console
query = wordCounts \
	.writeStream \
	.format("parquet").option("checkpointLocation","higgs/stage") \
	.start("higgs/stage")


query = wordCounts \
	.writeStream \
	.format("console") \
	.start()

query.awaitTermination()