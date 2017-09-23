#!/bin/sh

#benchs=(12 21 50 71 85)

for i in 12 21 50 71 85
do
  echo "running ${i}"
  (time hive --hiveconf hive.execution.engine=mr -f sample-queries-tpcds/query${i}.sql --database tpcds_text_db_1_50) 2> output/tpcds_query${i}_mr.out
done
