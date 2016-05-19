from pyspark import SparkContext
from pyspark import SparkConf
if __name__ == "__main__":

    conf = SparkConf()
conf.setMaster("local[2]")
conf.setAppName("GetHosts")

sc = SparkContext(conf = conf)

file = sc.textFile("/user/nagi/spark/WithoutCacheApplication.log")

counts = file.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("/user/nagi/spark/wordcount")