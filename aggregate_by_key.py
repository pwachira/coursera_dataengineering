from pyspark import SparkContext, SparkConf
import operator
import os


os.chdir()
#read in a local file
sc = SparkContext(conf = SparkConf().setAppName('App').setMaster('local'))
raw_data = sc.textFile('/data/twitter/twitter_sample_small.txt')

#define a method to read the data, split by tab
def parse_edge(s):
    user, follower = s.split('\t')
    return (int(user),int(follower))

# cache the intermediate rdd after parsing it
edges = raw_data.map(parse_edge).cache()

#apply aggregateByKey - see explanation below the code
fol_agg = edges.aggregateByKey(0,lambda v1,v2: v1+1 \
                               ,operator.add)

# top user/key with most followers.
# use operator to make sure the values(aggregated counts) and not the keys/userIds
#  are used for the comparison
top_user = fol_agg.top(1,operator.itemgetter(1))
print '%d %d'% (top_user[0][0], top_user[0][1])
sc.close()
