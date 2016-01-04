package com.seoeun.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * To run, bin/spark-submit --class com.seoeun.spark.streaming.SyslogFilter --master local[4] example-scala-0.9-SNAPSHOT.jar
 *
 * This example equivalent JavaSyslogFilter
 */
object SyslogFilter {
  def main (args: Array[String]) {
    val master = "local[2]"
    val appName = "SyslogFilter"
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(10))

    val inputDStream = ssc.socketTextStream("localhost", 9999)
    val filteredDStream = inputDStream.transform(rdd => SyslogFilter.filtered(rdd))

    //val words = inputDStream.flatMap(_.split(" "))
    //val pairs = words.map(word => (word, 1))
    //val wordCount = pairs.reduceByKey(_ + _)
    filteredDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def filtered(rDD: RDD[String]): RDD[(String)] = {
    rDD
      .filter(_.contains("onuLinkFault"))
  }

}
