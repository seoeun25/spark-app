from pyspark import SparkContext, SparkConf

import sys

class WordCount2:
    def getSparkContext(self, appName, master):
        print(appName)
        print(master)
        conf = SparkConf().setAppName(appName).setMaster(master)
        conf.set("spark.local.ip", "127.0.0.1")
        conf.set("spark.driver.host", "127.0.0.1")
        return SparkContext(conf=conf)

    def getInputRdd(self, sc, input):
        return sc.textFile(input)

    def process(self, inputRdd):
        words = inputRdd.flatMap(lambda s: s.split(" "))
        wcPair = words.map(lambda s: (s, 1))
        return wcPair.reduceByKey(lambda x, y: x + y)

if __name__ == "__main__":
    wc = WordCount2()
    sc = wc.getSparkContext("WordCount2", sys.argv[1])
    inputRdd = wc.getInputRdd(sc, sys.argv[2])
    resultRdd = wc.process(inputRdd)
    resultRdd.saveAsTextFile(sys.argv[3])
    sc.stop()

