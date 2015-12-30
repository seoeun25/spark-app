package com.seoeun.spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * To run, bin/spark-submit --class com.seoeun.spark.streaming.NetworkWordCount --master local[4] example-scala-0.9-SNAPSHOT.jar
 *
 * This example equivalent JavaNetworkWordCount
 */
object NetworkWordCount {
  def main (args: Array[String]) {
    val master = "local[2]"
    val appName = "NetworkWordCount"
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(10));

    val lines = ssc.socketTextStream("localhost", 9999);
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCount = pairs.reduceByKey(_ + _)
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
