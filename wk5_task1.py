from pyspark import SparkConf, SparkContext
from operator import add
sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local"))

import re

def parse_article(line):
    try:
        article_id, text = unicode(line.rstrip()).split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
        words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
        bigram = []
        for idx, word in enumerate(words):
            if word.lower() == 'narodnaya' and idx < len(words) - 1 :
                bigram.append(word.lower() + '_' + words[idx+1].lower())
        return bigram 
    except ValueError as e:
        return []

wiki = sc.textFile("/data/wiki/en_articles_part/articles-part", 16).map(
     parse_article).filter(
        lambda a:len(a) > 0).flatMap(
            lambda a:a).map(lambda w:(w,1)).reduceByKey(add).sortByKey()
for name, num in wiki.collect():
    print '%s\t%d' % (name, num)
