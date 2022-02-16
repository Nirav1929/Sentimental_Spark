'''
Reinstall pyspark 2.4.6 for KafkaUtils
pip install --force-reinstall pyspark==2.4.6
'''
from pprint import pprint

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)  # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")

    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    This function plots the counts of positive and negative words for each timeSeriesstep.
    """
    positive = []
    negative = []
    timeSeries = []

    for val in counts:
        key1 = val[0]
        positive.append(key1[1])
        key2 = val[1]
        negative.append(key2[1])

    for i in range(len(counts)):
        timeSeries.append(i)

    line1 = plt.plot(timeSeries, positive,"gD-", linewidth=5 , label='Positive')
    line2 = plt.plot(timeSeries, negative,'rD-', linewidth=5,  label='Negative')
    #plt.axis([0, len(counts), 0, max(max(positive), max(negative))+50])
    plt.xlabel('Time')
    plt.ylabel('Count')
    plt.show()
    plt.savefig('plot.png')

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    wordList = []
    with open(filename, 'r') as f:
        for line in f.readline():
            wordList.append(line.strip())
    return wordList


def getRunningCount(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount) 

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
    ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])


    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).

    words = tweets.flatMap(lambda line:line.split(" "))
    positive = words.map(lambda word: ('Positive', 1) if word in pwords else ('Positive', 0))
    negative = words.map(lambda word: ('Negative', 1) if word in nwords else ('Negative', 0))
    cuml = positive.union(negative)
    each_count = cuml.reduceByKey(lambda x,y: x+y)
    each_count.pprint()
    running_counts = each_count.updateStateByKey(getRunningCount)
    running_counts.pprint()
    counts = []
    each_count.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    ssc.start() 
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully = True)

    return counts


if __name__ == "__main__":
    main()
