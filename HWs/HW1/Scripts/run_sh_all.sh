my_script = Software_deployment.sh

ssh vm-32-2 'bash -s' < ${my_script}
ssh vm-32-3 'bash -s' < ${my_script}
ssh vm-32-4 'bash -s' < ${my_script}
ssh vm-32-5 'bash -s' < ${my_script}

