package com.seoeun.spark.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}

/**
 * To run, bin/spark-submit --class com.seoeun.spark.streaming.SqlNetworkWordCount --master local[4] example-scala-0.9-SNAPSHOT.jar
 *
 * $ nc -lk 9999
 * hello world
 *
 * This example equivalent JavaSqlNetworkWordCount
 */

object SqlNetworkWordCount {
  def main (args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println("Usage: NetworkWordCount <hostname> <port>")
//      System.exit(1)
//    }

    // Create the context with a 2 second batch size
    val appName = "SqlNetworkWordCount"
    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val hostName = "localhost"
    val port = 9999

    val lines = ssc.socketTextStream(hostName, port, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD((rdd: RDD[String], time: Time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

      // Convert RDD[String] to RDD[case class] to DataFrame
      import sqlContext.implicits._
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Register as table
      wordsDataFrame.registerTempTable("words")

      // Do word count on table using SQL and print it
      val wordCountDataFame = sqlContext.sql("select word, count(*) as total from words group by word")
      println(s"\n ****** =========== $time ========= *****\n")
      wordCountDataFame.show()
    })

    ssc.start()
    ssc.awaitTermination()

  }

  // Case clss for converting RDD to DataFrame
  case class Record(word:String)

  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext:SparkContext) : SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }

  }
}



