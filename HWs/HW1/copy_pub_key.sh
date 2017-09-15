cat ~/.ssh/id_rsa.pub | ssh -i group32.pem ubuntu@vm-32-2 'cat >> .ssh/authorized_keys'
cat ~/.ssh/id_rsa.pub | ssh -i group32.pem ubuntu@vm-32-3 'cat >> .ssh/authorized_keys'
cat ~/.ssh/id_rsa.pub | ssh -i group32.pem ubuntu@vm-32-4 'cat >> .ssh/authorized_keys'
cat ~/.ssh/id_rsa.pub | ssh -i group32.pem ubuntu@vm-32-5 'cat >> .ssh/authorized_keys'

#scp -i group32.pem /home/ubuntu/.ssh/id_rsa.pub ubuntu@10.254.0.188:~/.ssh/authorized_keys
#scp -i group32.pem /home/ubuntu/.ssh/id_rsa.pub ubuntu@10.254.0.213:~/.ssh/authorized_keys
#scp -i group32.pem /home/ubuntu/.ssh/id_rsa.pub ubuntu@10.254.0.190:~/.ssh/authorized_keys
#scp -i group32.pem /home/ubuntu/.ssh/id_rsa.pub ubuntu@10.254.0.191:~/.ssh/authorized_keys
