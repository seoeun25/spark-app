package com.seoeun.spark.simpleapp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * To run, bin/spark-submit --class com.seoeun.spark.simpleapp.ScalaWordCount --master local[4] example-scala-0.9-SNAPSHOT.jar
 */
object ScalaWordCount {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Scala Word Count")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("hdfs://sembp:8020/user/seoeun/spark/word-input");
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://sembp:8020/user/seoeun/spark/word-output2");
  }
}
