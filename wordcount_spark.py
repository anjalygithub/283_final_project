import sys
from pyspark import SparkContext, SparkConf
import datetime

if __name__ == "__main__":

	starttime=datetime.datetime.now()
	
	conf = SparkConf().setAppName("Spark Count")
	sc = SparkContext(conf=conf)

#file = sc.textFile("file:///Users/anjalynedumala/Documents/Masters/courseWork/283/movies_50M.csv")
	file = sc.textFile("file:///Users/anjalynedumala/Documents/Masters/courseWork/283/reviews_large.csv")
	tokenized = file.flatMap(lambda line: line.split(","))
	wordCounts = tokenized.map(lambda word: (word, 1))
	add = wordCounts.reduceByKey(lambda v1,v2:v1 +v2)
	
	list = add.collect()
	print repr(list)[1:-1]
	
	endtime=datetime.datetime.now()
	time_diff = endtime-starttime
	time = str(time_diff)
	print "execution time = " + time

	sc.stop()
