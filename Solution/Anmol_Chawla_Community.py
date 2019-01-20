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
import itertools
#from networkx import girvan_newman

# Setup
sc = SparkContext('local[*]','comm')

# Input arguments
ipfile = sys.argv[1]
filename = "Anmol_Chawla_Community.txt"



data = sc.textFile(ipfile)



# Check if set makes a differnece or not
top  = data.first()
data = data.filter(lambda x : x != top).cache()
usermovie = data.map(lambda line:line.split(",")).map(lambda x:(int(x[0]),int(x[1]))).groupByKey().sortByKey().map(lambda x:( x[0],set(x[1]))  ).filter(lambda x: len(x[1]) >= 9).collect()
users = data.map(lambda line:line.split(",") ).map(lambda x: (int(x[0]),1) ).distinct().sortByKey().map(lambda x: x[0])

# Finding pairs which are an edge
edges = []
for i in range(len(usermovie)):
  for j in range(len(usermovie)):
    if ( (usermovie[i][0] != usermovie[j][0]) and (len(usermovie[i][1].intersection(usermovie[j][1])) >= 9 ) ):
      hold = (usermovie[i][0], usermovie[j][0])
      edges.append(hold)

# Running BFS and finding betweeness
OG = nx.Graph()
OG.add_edges_from(edges)
result = edge_betweenness_centrality(OG, k=None, normalized=False, weight=None, seed=None)
bw = result.items()
bw.sort(key = lambda x: x[1], reverse= True)

bet = []
for i in bw:
  val = (int(i[1]*1000)/float(1000))
  hold = (i[0],val)
  bet.append(hold)


values = sc.parallelize(bet)

rdd = values.map(lambda x: (x[1],x[0])).groupByKey().sortByKey(ascending=False).map(lambda x: (x[0],list(x[1]))).collect()

G = OG.copy()
# creating look up
lut = defaultdict(dict)
for i in bw:
  lut[i[0][0]][i[0][1]] = i[1]
  lut[i[0][1]][i[0][0]] = i[1]
  
def modularity(G):
  global lut
  global base 
  global EG
  part1 = []
  add = 0

  for a in G:
    
    if a in EG:
      aij = 1
    else:
      aij = 0
       
    ki = len(lut[a[0]])
    kj = len(lut[a[1]])
    
  
    hold = aij - (float(ki*kj)/base) 
      
    
    add = add + hold
    
    
    
  
  op = add/base
  
  
  return op

# Making a cut
res= []
temp = []
mem = OrderedDict()
EG = {}

holder = list(G.edges())
base = 2 * len(holder)

for i in holder:
  EG[i] = 1

original = list(G.nodes())
original_pair = list(itertools.combinations(original, 2))
#print("original_pair", len(original_pair))
old_m = modularity(original_pair)
print("Original modularity",old_m)
mem[tuple(original_pair)] = old_m
old = nx.number_connected_components(G)

start_all = time.time()

for i in rdd:
  to_remove = i[1]
  for i in to_remove:
    G.remove_edge(*i)
    
    
  new = nx.number_connected_components(G)

  if old < new:
    old = new   
    # calculate modularity
    #start2 = time.time()
    ans = list(nx.connected_component_subgraphs(G))
    modul = 0
    for i in ans:
      pair = list(itertools.combinations(list(i), 2))
      if pair == []:
        m = 0
      else:
        check = tuple(pair)
        if check in mem:
          m = mem[check]
        else:
          m = modularity(pair)
          mem[check] = m
     
       
      modul = modul + m 
      

    if modul > old_m:
      old_m = modul
      best_G = G.copy()

      
      
stop_all = time.time()

print("Time",(stop_all- start_all))



ans = list(nx.connected_component_subgraphs(best_G))

sol = []
for i in ans:
  hold = sorted(list(i))
  sol.append(hold)
  
  
finals = sorted(sol)


with open(filename, 'w') as caseop:
	for i in finals:
		hold = str(i).replace(" ","")
		print(hold)
		caseop.write("{}\n".format(hold))

