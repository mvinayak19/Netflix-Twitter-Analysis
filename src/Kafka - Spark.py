from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sparkc = SparkContext(conf=conf)

    # Creating a streaming context with batch interval of 100 sec
    stcon = StreamingContext(sparkc, 100)
    stcon.checkpoint("checkpoint")
    pos_words = load_wordlist("positive-words.txt")
    neg_words = load_wordlist("negative-words.txt")
    counts = stream(stcon, pos_words, neg_words, 100)
    make_plot(counts)
	filename="words.txt"


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

    posLine = plt.plot(time, positiveCounts, 'bo-', label='Positive')
    negLine = plt.plot(time, negativeCounts, 'go-', label='Negative')
    plt.axis([0, len(counts), 0, max(max(positiveCounts), max(negativeCounts)) + 50])
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.legend(loc='upper left')
    plt.show()


def load_wordlist(filename):
    """
    This function returns a list or set of words from the given filename.
    """
    words = {}
    f = open(filename, 'rU')
    text = f.read()
    text = text.split('\n')
    for line in text:
        words[line] = 1
    f.close()
    return words


def wordSentiment(word, pos_words, neg_words):
    if word in pos_words:
        return ('positive', 1)
    elif word in neg_words:
        return ('negative', 1)


def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


def sendRecord(record):
    connection = createNewConnection()
    connection.send(record)
    connection.close()


def stream(stcon, pos_words, neg_words, duration,filename):
    kstream = KafkaUtils.createDirectStream(
        stcon, topics=['tweets'], kafkaParams={"metadata.broker.list": 'localhost:9092'}) #accessing the "tweets" topic
    tweets = kstream.map(lambda x: x[1].encode("ascii", "ignore"))
    f = open(filename, 'wb')  
    # Tokensize te tweets to get running count
	# Store the words for creating word cloud
    words = tweets.flatMap(lambda line: line.split(" "))
	f.write(words)
    positive = words.map(lambda word: ('Positive', 1) if word in pos_words else ('Positive', 0))
    negative = words.map(lambda word: ('Negative', 1) if word in neg_words else ('Negative', 0))
    allSentiments = positive.union(negative)
    sentimentCounts = allSentiments.reduceByKey(lambda x, y: x + y)
    runningSentimentCounts = sentimentCounts.updateStateByKey(updateFunction)
    runningSentimentCounts.pprint()

    
    counts = []
    sentimentCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))

    # Starting the computation
    stcon.start()
    stcon.awaitTerminationOrTimeout(duration)
    stcon.stop(stopGraceFully=True)

    return counts


if __name__ == "__main__":
    main()