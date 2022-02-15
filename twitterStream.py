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
    This function plots the counts of positive and negative words for each timestep.
    """
    positiveCounts = []
    negativeCounts = []
    time = []

    for val in counts:
        positiveTuple = val[0]
        positiveCounts.append(positiveTuple[1])
        negativeTuple = val[1]
        negativeCounts.append(negativeTuple[1])

    for i in range(len(counts)):
        time.append(i)

    posLine = plt.plot(time, positiveCounts,'bo-', label='Positive')
    negLine = plt.plot(time, negativeCounts,'go-', label='Negative')
    plt.axis([0, len(counts), 0, max(max(positiveCounts), max(negativeCounts))+50])
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.legend(loc = 'upper left')
    plt.show()
    plt.savefig('graph.png')

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    word_list = []
    with open(filename, 'r') as f:
        for line in f.readline():
            word_list.append(line.strip())

    return word_list


def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount) 

def stream1(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics=['twitterstream'], kafkaParams={"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    print(tweets, type(tweets))
    counts = []
    ssc.start()

    pos_tweets, neg_tweets = 0, 0
    for tweet in tweets:
        if tweet in pos_tweets:
            pos_tweets+=1

        if tweet in neg_tweets:
            neg_tweets+=1

    pprint(pos_tweets, neg_tweets)
    counts.append([('positive', pos_tweets), ('negative', neg_tweets)])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE

    # Let the counts varable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))

    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
    ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    print("Hello this is bad ",  tweets)
    # Each element of tweets will be the text of a tweet.
    # We keep track of a running total counts and print it at every time step.
    words = tweets.flatMap(lambda line:line.split(" "))
    positive = words.map(lambda word: ('Positive', 1) if word in pwords else ('Positive', 0))
    negative = words.map(lambda word: ('Negative', 1) if word in nwords else ('Negative', 0))
    allSentiments = positive.union(negative)
    sentimentCounts = allSentiments.reduceByKey(lambda x,y: x+y)
    sentimentCounts.pprint()
    runningSentimentCounts = sentimentCounts.updateStateByKey(updateFunction)
    runningSentimentCounts.pprint()
    
    # The counts variable hold the word counts for all time steps
    counts = []
    sentimentCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    
    # Start the computation
    ssc.start() 
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully = True)

    return counts
if __name__ == "__main__":
    main()
