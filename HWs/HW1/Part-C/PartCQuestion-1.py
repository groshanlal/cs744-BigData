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
    if len(sys.argv) !=3:
        print("Usage: pagerank <file>", file = sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("CS-744-Assignment1-PartC-Question1")\
        .config("spark.eventLog.enabled","true")\
        .config("spark.eventLog.dir","hdfs://10.254.0.212:8020/user/ubuntu/log")\
        .config("spark.executor.memory","1g")\
        .config("spark.driver.memory","1g")\
        .config("spark.executor.cores","4")\
        .config("spark.task.cpus","1")\
        .master("spark://10.254.0.212:7077")\
        .getOrCreate()

# Read all the data file as a RDD type

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r:r[0])

# Remvoe the head description start with a # of the origin file

    lines = lines.filter(lambda line: "#" not in line)

# Links between From-Node to To-Node in order 
# Return (u'287144', <pyspark.resultiterable.ResultIterable object at 0x7f3dafc46550>) 

    links = lines.map(lambda line: (line.split("\t")[0], line.split("\t")[1])).distinct().groupByKey()
# Build Ranks for pages and initial them as 1.0

    ranks = links.map(lambda line: (line[0], 1.0)) 

# 10 interation for calculating the Rank

    for iteration in range(int(sys.argv[2])):

        Contributes = links.join(ranks)\
                    .flatMap(lambda line: computeContributes(line[1][0], line[1][1]))
        
        ranks = Contributes.reduceByKey(add).mapValues(lambda rank: rank*0.85 + 0.15)

    for(link, rank) in ranks.collect():
        print("%s has rank: %s. " %(link, rank))

    spark.stop()
