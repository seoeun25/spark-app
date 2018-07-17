package com.seoeun.ch1.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author seoeun
  * @since ${VERSION} on 6/5/17
  */
object WordCount1 {

  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Usage: WordCount1 <master> <input> <output>")

    val sc = getSparkContext("WordCount1", args(0))

    val inputRdd = getInputRdd(sc, args(1))

    val resultRdd = process(inputRdd)

    saveToLocal(resultRdd, args(2))

  }

  def getSparkContext(appName: String, master: String) = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    new SparkContext(conf)
  }

  def getInputRdd(sc: SparkContext, input: String) = {
    sc.textFile(input)
  }

  def process(inputRdd: RDD[String]) = {
    val words = inputRdd.flatMap(str => str.split(" "))
    val wcPair = words.map((_, 1))
    wcPair.reduceByKey(_ + _)
  }

  def saveToLocal(resultRdd:RDD[(String, Int)], output: String): Unit = {
    resultRdd.saveAsTextFile(output)
  }

}
