'''
Created on 11 May 2017

@author: xuepeng
'''

from pyspark import SparkContext
from pyspark.streaming.context import StreamingContext

from algorithm.OnlineModel4Spark import OnlineModel4Spark
import sys


if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: hdfs_wordcount.py <directory>"
        exit(-1)

    numFeatures = 57
    numClasses = 5
    learningRatio = 1.0
    
    sc = SparkContext("local[20]", "NetworkWordCount")
    ssc = StreamingContext(sc, 3)

# the parameter of textFileStream is directory of HDFS, not a standard Linux or windows Directory    
    lines = ssc.textFileStream(sys.argv[1])
    
    model = OnlineModel4Spark(numFeatures,numClasses,learningRatio)
    model.initWeight()
    model.trainOn(lines)
      
    print model.getWeight()
    
    ssc.start()
    ssc.awaitTermination()
    
    
    

     

