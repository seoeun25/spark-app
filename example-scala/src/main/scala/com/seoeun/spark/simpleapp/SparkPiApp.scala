package com.seoeun.spark.simpleapp

import org.apache.spark.{SparkContext, SparkConf}

import scala.math.random

/**
 * To run, bin/spark-submit --class com.seoeun.spark.simpleapp.SparkPiApp --master local[4] example-scala-0.9-SNAPSHOT.jar 100
 */
object SparkPiApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi App By azrael")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 -1
      val y = random * 2 -1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }

}
