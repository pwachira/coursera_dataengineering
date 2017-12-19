from pyspark import SparkContext, SparkConf
# try:
    # sc.close()
# except e:
   # print e
   # print 'sc doesnt exist or cannot be closed'
sc = SparkContext(conf= SparkConf().setAppName("App").setMaster('local'))

def parse_edge(s):
  user, follower = s.split("\t")
  return (int(user), int(follower))

def step(item):
  prev_d, next_v = item[1][0], item[1][1]
  return (next_v, prev_d + 1)
def complete(item):
  v, old_d, new_d = item[0], item[1][0], item[1][1]
  return (v, old_d if old_d is not None else new_d)
import pdb; pdb.set_trace()
n = 2  # number of partitions
edges = sc.textFile("/data/twitter/twitter_sample_xs.txt").map(parse_edge).cache()
forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(n).persist()

# x = 12
x = 2
d = 0
distances = sc.parallelize([(x, d)]).partitionBy(n)
while True:
  candidates_pre = distances.join(forward_edges, n)
  candidates = candidates_pre.map(step)
  new_distances_pre = distances.fullOuterJoin(candidates, n)
  new_distances = new_distances_pre.map(complete, True).persist()
  count = new_distances.filter(lambda i: i[1] == d + 1).count()
  if count > 0:
    d += 1
    distances = new_distances
    print("d = ", d, "count = ", count)
  else:
    break
