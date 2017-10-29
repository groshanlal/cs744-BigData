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
	.readStream \
	.parquet("higgs/stage/out/")


# Generate running word count
wordCounts = activity.groupBy("userB").count()
	
			
query = wordCounts \
	.writeStream.trigger(processingTime='10 minutes') \
	.format("console") \
	.start()

query.awaitTermination()