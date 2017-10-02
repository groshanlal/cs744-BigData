# IP_ADR="1.2.3.4"


runTime=10


coef=0.25
firstQ=$(echo "${runTime} * ${coef}" | bc)
thirdQ=$(echo "${runTime} * (1-${coef})" | bc)

sleep ${firstQ}


# read the last application ID from the out file
applicationID=`cat /home/ubuntu/workload/hive-tpcds-tpch-workload/output/tpcds_query71_mr.out | grep -oP "application_[^_]*_[^_^/]*" | tail -1`

IP_ADR=`yarn logs -applicationId ${applicationID} | grep -oP "vm-32[^_]*" | tail -1`


# block the IP address
sudo iptables -I INPUT -s ${IP_ADR} -j DROP


sleep ${runTime}


# unblock the IP address
sudo iptables -D INPUT -s ${IP_ADR} -j DROP