from __future__ import print_function # Has to be at the very first line

import re
import sys
from operator import add

from pyspark.sql import SparkSession

def computeContributes(start, rank):
    """Calculate the rank contributes from To_Node to From_Node"""
    ends = len(start)
    for node in start:
        yield(node, rank/ends)


if __name__ == "__main__":
    if len(sys.argv) !=4:
        print("Usage: pagerank <file> <iterations> <partitions>", file = sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("CS-744-Assignment1-PartC-2")\
        .config("spark.eventLog.enabled","true")\
        .config("spark.eventLog.dir","hdfs://10.254.0.212:8020/user/ubuntu")\
        .config("spark.executor.memory","1g")\
        .config("spark.driver.memory","1g")\
        .config("spark.executor.cores","4")\
        .config("spark.task.cpus","1")\
        .getOrCreate()

    # Read all the data file as a RDD type
    partitions = int(sys.argv[3])
    #lines = spark.read.text(sys.argv[1]).rdd.map(lambda r:r[0])
    lines = spark.sparkContext.textFile(sys.argv[1], partitions)
    # Remvoe the head description start with a # of the origin file

    lines = lines.filter(lambda line: "#" not in line)

    #partitions = int(sys.argv[3])
    # Links between From-Node to To-Node in order
    # Return (u'287144', <pyspark.resultiterable.ResultIterable object at 0x7f3dafc46550>)

    links = lines.map(lambda line: (line.split("\t")[0], line.split("\t")[1])).distinct().groupByKey().partitionBy(partitions)
    # Build Ranks for pages and initial them as 1.0

    ranks = links.map(lambda line: (line[0], 1.0)).partitionBy(partitions)

    # 10 interation for calculating the Rank

    for iteration in range(int(sys.argv[2])):

        Contributes = links.join(ranks)\
                    .flatMap(lambda line: computeContributes(line[1][0], line[1][1]))

        ranks = Contributes.reduceByKey(add).mapValues(lambda rank: rank*0.85 + 0.15)

    for(link, rank) in ranks.collect():
        print("%s has rank: %s. " %(link, rank))
