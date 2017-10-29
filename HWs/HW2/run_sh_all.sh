to_run=$1
#################################
cur=$(pwd)
my_script="${cur}/${to_run}"

echo ${my_script}

cd ~
${my_script}
cd -

./${my_script}
ssh vm-32-2 'bash -s' < ${my_script}
ssh vm-32-3 'bash -s' < ${my_script}
ssh vm-32-4 'bash -s' < ${my_script}
ssh vm-32-5 'bash -s' < ${my_script}

