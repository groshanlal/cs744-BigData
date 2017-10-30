hadoop fs -rm higgs/stage/*

count=1
stage_dir="higgs/stage" # $1
input_dir="higgs/split-dataset"

spark-submit PartAQuestion1.py ${stage_dir}  &

while :
do
	echo ">>> copy file ${input_dir}/${count}.csv to staging folder!"
	
	hadoop fs -cp "${input_dir}/${count}.csv" ${stage_dir}
	sleep 5
	((count+=1))
done



