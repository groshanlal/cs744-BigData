to_run=PartC1.py
filedir=/home/ubuntu
my_script="${filedir}/${to_run}"
echo ${my_script}

fileName=$1
numOfLoop=$2
spark-submit --py-files ${my_script} fileName numOfLoop