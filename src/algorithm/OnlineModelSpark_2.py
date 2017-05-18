'''
Created on 11 May 2017

@author: xuepeng
'''

from pyspark import SparkContext
from pyspark.streaming.context import StreamingContext

from algorithm.PerceptronAlgorithm import PerceptronAlgorithmSpark
import sys


# from algorithm.PerceptronAlgorithm import PerceptronAlgorithmSpark
def printCount(rdd):
    print rdd.count()


if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: hdfs_wordcount.py <directory>"
        exit(-1)

    numFeatures = 57
    numClasses = 5
    learningRatio = 1.0
    
    sc = SparkContext("local[20]", "NetworkWordCount")
    
#TODO Convert existing RDD to DStream

    ssc = StreamingContext(sc, 3)
#     lines = ssc.textFileStream("~/uts/hdfs/")
    lines = ssc.textFileStream(sys.argv[1])
#     lines = lines.map(lambda item : item.split(","))
     
    model = PerceptronAlgorithmSpark(numFeatures,numClasses,learningRatio)
    model.initWeight()
    model.trainOn(lines)
      
    print model.getWeight()

#     lines.foreachRDD(printCount)
    
    ssc.start()
    ssc.awaitTermination()
    
    
    

     

