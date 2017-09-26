

def pageRank(filename):
	neighbors = {}
	with open (filename) as file:
		for line in file:
			
			line = line.strip()
			if line.startswith('#'):
				continue;
			if line[0] in neighbors:
				neighbors[line[0]] += ' '
				neighbors[line[0]] += str(line[2])
#% line[1] is \t
			else:
				neighbors[line[0]] = str(line[2])
	rank ={}
	rank.fromkeys(neighbors.keys(), 1.0)	
	print (len(rank))
	
	contribution = {}

	loopNum = 10
# update after calculating all contributions
	for i in range(loopNum):
		contribution.fromkeys(neighbors.keys(),0)
		for index in rank:
			contribution= rank[index] / len( neighbors[index])
			for n in neighbors[index]:
				contribution[int(n)] += contribution
		for index in rank:
			rank[index] = 0.15 + 0.85 * contribution[index]
	

if __name__ == "__main__":
	filename = "../web-BerkStan.txt"
	pageRank(filename)
