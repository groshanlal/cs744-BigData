import numpy as np

M = 3;
N =5;
l = np.zeros([M,M])

def get_machine_id(i,j,N):
		return  (i+j)%N

for i in range(M):
	for j in range(M):
		l[i][j] = (i+j)%N
		
print np.histogram(l,N)

print l