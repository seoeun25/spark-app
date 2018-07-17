package com.seoeun.ch1.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Assert, Test}

import scala.collection.mutable.ListBuffer

/**
  * @author seoeun
  * @since ${VERSION} on 6/6/17
  */
class WordCount1Test {

  @Test
  def test(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("WordCount1Test")
    conf.set("spark.local.ip", "127.0.0.1")
    conf.set("spark.driver.host", "127.0.0.1")

    val sc = new SparkContext(conf)
    val input = new ListBuffer[String]
    input += "Apache Spark is a fast and general engine for large-scale data processing."
    input += "Spark runs on both Windows and UNIX-like systems"
    input.toList

    val inputRdd = sc.parallelize(input)
    val resultRdd = WordCount1.process(inputRdd)
    val resultMap = resultRdd.collectAsMap()

    Assert.assertEquals(2, resultMap.get("Spark").get)
    Assert.assertEquals(2, resultMap.get("and").get)
    Assert.assertEquals(1, resultMap.get("runs").get)
    Assert.assertEquals(None, resultMap.get("seoeun"))

    print(resultMap)

    sc.stop()

  }

}
