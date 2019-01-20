from pyspark import SparkContext
from collections import defaultdict
from collections import OrderedDict
import sys
import os
from itertools import chain, combinations
from operator import add
from itertools import izip
from pyspark.sql import SparkSession
import itertools
import time
import math
from collections import OrderedDict
from collections import Counter
from random import randint
import networkx as nx
from networkx import edge_betweenness_centrality

sc = SparkContext('local[*]','bw')
# Setup

# Input arguments
ipfile = sys.argv[1]
filename = "Anmol_Chawla_Betweenness.txt"


start = time.time()
data = sc.textFile(ipfile)

# Check if set makes a differnece or not
top  = data.first()
data = data.filter(lambda x : x != top).cache()
usermovie = data.map(lambda line:line.split(",")).map(lambda x:(int(x[0]),int(x[1]))).groupByKey().sortByKey().map(lambda x:( x[0],set(x[1]))  ).filter(lambda x: len(x[1]) >= 9).collect()
# users = data.map(lambda line:line.split(",") ).map(lambda x: (int(x[0]),1) ).distinct().sortByKey().map(lambda x: x[0])

# Finding pairs which are an edge
edges = []
for i in range(len(usermovie)):
  for j in range(len(usermovie)):
    if ( (usermovie[i][0] != usermovie[j][0]) and (len(usermovie[i][1].intersection(usermovie[j][1])) >= 9 ) ):
      hold = (usermovie[i][0], usermovie[j][0])
      edges.append(hold)

# Running BFS and finding betweeness
G = nx.Graph()
G.add_edges_from(edges)
result = edge_betweenness_centrality(G, k=None, normalized=False, weight=None, seed=None)
solution = sorted(result.items())

stop = time.time()

print("Time: {}".format(stop-start))

#Writing results to file
with open(filename, 'w') as caseop:
	for i in solution:
		one = str(i[0][0])
		two = str(i[0][1])
		three = str(i[1])
		caseop.write("{},{},{}".format(one,two,three))
		caseop.write("\n")

