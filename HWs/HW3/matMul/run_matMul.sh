#!/bin/bash
export TF_LOG_DIR="/home/ubuntu/tf/logs"

#!/bin/bash

# tfdefs.sh has helper function to start process on all VMs
# it contains definition for start_cluster and terminate_cluster
source tfdefs.sh

# startserver.py has the specifications for the cluster.
start_cluster startserver.py

echo "Executing the distributed tensorflow job from exampleDistributed.py"
# testdistributed.py is a client that can run jobs on the cluster.
# please read testdistributed.py to understand the steps defining a Graph and
# launch a session to run the Graph
python matMulDist.py

# defined in tfdefs.sh to terminate the cluster
terminate_cluster


# start the tensorboard web server. If you have started the webserver on the VM
# a public ip, then you can view Tensorboard on the browser on your workstation
# (not the CloudLab VMs). Navigate to http://<publicip>:6006 on your browser and
# look under "GRAPHS" tab.

# Under the "GRAPHS" tab, use the options on the left to navigate to the "Run" you are interested in.
tensorboard --logdir=$TF_LOG_DIR
