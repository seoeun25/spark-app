package com.seoeun.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * To run, bin/spark-submit --class com.seoeun.spark.streaming.StringFilter --master local[4] example-scala-0.9-SNAPSHOT.jar
 *
 * This example equivalent JavaNetworkWordCount
 */
object StringFilter {
  def main (args: Array[String]) {
    val master = "local[2]"
    val appName = "StringFilter"
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(10));

    val inputDStream = ssc.socketTextStream("localhost", 9999);
    val errCountStream = inputDStream.transform(rdd => StringFilter.countErrors(rdd))
    errCountStream.foreachRDD(rdd => {
      if (rdd.isEmpty()) {
        System.out.println("---- Errors this minute: rdd is empty");
      } else {
        System.out.println("---- Errors this minute: %d".format(rdd.first()._2));
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def countErrors(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd
      .filter(_.contains("ERROR")) // Keep "ERROR" lines
      .map(s => (s.split(" ")(0), 1)) // return tuple with data & count
      .reduceByKey(_+_) // Sum counts for each date
  }

}
