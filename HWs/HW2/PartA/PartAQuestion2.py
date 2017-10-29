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

#.option("inferSchema", "true")

# Split the lines into words
# words = lines.select(
# 	explode(
# 		split(lines.value, " ")
# 	).alias("word")
# )

# Generate running word count
wordCounts = activity.select("userB").where("interaction = MT")
				


# Start running the query that prints the running counts to the console
query = wordCounts \
	.writeStream \
	.format("parquet").option("checkpointLocation","higgs/stage") \
	.start("higgs/stage")

query.awaitTermination()