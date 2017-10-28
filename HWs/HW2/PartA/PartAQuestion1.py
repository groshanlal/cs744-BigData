from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import StructType

spark = SparkSession \
	.builder \
	.appName("StructuredNetworkWordCount") \
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
	.add("timestamp", "Timestamp")\
	.add("interaction","string")

activity = spark \
	.readStream \
	.option("sep", ",") \
	.option("inferSchema", "true")
	.csv("higgs/stage")  
	# Equivalent to format("csv").load("/path/to/directory")
	#.schema(userSchema) \
	


# Split the lines into words
# words = lines.select(
# 	explode(
# 		split(lines.value, " ")
# 	).alias("word")
# )

# Generate running word count
wordCounts = activity.groupBy(
				window(activity.timestamp, "1 hours", "30 minutes"),
				"interaction"
			).count()



# Start running the query that prints the running counts to the console
query = wordCounts \
	.writeStream \
	.outputMode("complete") \
	.format("console") \
	.start()

query.awaitTermination()