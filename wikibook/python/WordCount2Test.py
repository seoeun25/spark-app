import unittest
from pyspark import SparkContext, SparkConf

from WordCount2 import WordCount2


class WordCount2Test(unittest.TestCase):
    def testWordCount(self):
        wc = WordCount2()
        sc = wc.getSparkContext("WordCount2Test", "local[*]")
        input = ["Apache Spark is a fast and general engine for large-scale data processing.",
                 "Spark runs on both Windows and UNIX-like systems"]
        inputRdd = sc.parallelize(input)
        resultRdd = wc.process(inputRdd)
        resultMap = resultRdd.collectAsMap()

        self.assertEqual(resultMap['Spark'], 2)
        self.assertEqual(resultMap['UNIX-like'], 1)
        self.assertEqual(resultMap['runs'], 1)
        #self.assertEqual(resultMap['seoeun'], None)

        print(resultMap)

        sc.stop()

