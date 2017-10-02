# IP_ADR="1.2.3.4"


runTime=328.505
#runTime=10.0

coef=0.25
firstQ=$(echo "${runTime} * ${coef}" | bc)
thirdQ=$(echo "${runTime} * (1-${coef})" | bc)

#./run_query_hive_mr.sh $

echo "Sleeping"

sleep ${firstQ}


# read the last application ID from the out file
applicationID=`cat /home/ubuntu/workload/hive-tpcds-tpch-workload/output/tpcds_query71_mr.out | grep -oP "application_[^_]*_[^_^/]*" | tail -1`

IP_ADR=`yarn logs -applicationId ${applicationID} | grep "Container: container_" | grep -oP "vm-32[^_]*" | tail -1`

echo "Killing: $IP_ADR"

# block the IP address
sudo iptables -I INPUT -s ${IP_ADR} -j DROP


#sleep ${runTime}


# unblock the IP address
sudo iptables -D INPUT -s ${IP_ADR} -j DROP
