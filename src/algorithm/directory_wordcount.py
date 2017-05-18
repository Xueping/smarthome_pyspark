import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def printCount(item):
    print item.count()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: hdfs_wordcount.py <directory>"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingHDFSWordCount")
    ssc = StreamingContext(sc, 1)

    lines = ssc.textFileStream(sys.argv[1])
    
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda x: (x, 1))\
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()