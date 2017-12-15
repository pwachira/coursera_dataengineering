from pyspark import SparkConf, SparkContext
from operator import add
import re

sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster('spark://WCHR-HP-01:7077'))


def parse_article(line):
    try:
        article_id, text = unicode(line.rstrip()).split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
        words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
        bigram = [(words[i].lower(),words[i+1].lower()) for i in range(
               0,len(words) - 1) if words[i].lower() == 'narodnaya']
        return bigram
    except ValueError as e:
        print e
        return []

wiki = sc.textFile(
           "/data/wiki/en_articles_part/articles-part", 16).flatMap(
           parse_article).map(
           lambda w:(w,1)).reduceByKey(
           add).sortByKey()

for name, num in wiki.collect():
    print '%s\t%d' % (name, num)
