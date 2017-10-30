# hadoop fs -rm higgs/stage/*

stage_dir="higgs/stage/out" # $1
input_dir="higgs/split-dataset"

spark-submit PartAQuestion3.py ${stage_dir} 

