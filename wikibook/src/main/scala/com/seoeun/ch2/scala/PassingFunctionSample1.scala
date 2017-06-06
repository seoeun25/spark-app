package com.seoeun.ch2.scala

import org.apache.spark.SparkContext

/**
  * @author seoeun
  * @since ${VERSION} on 6/6/17
  */
class PassingFunctionSample1 {

  val count = 1

  def add(i: Int): Int = {
    count + i
  }

  def runMapSample(sc: SparkContext) = {
    val rdd1 = sc.parallelize(1 to 10)
    // NoSerializationException : 클러스터를 구성하는 각 서버에서 동작할 수 있도록 모든 서버에 전댈되어야 한다.
    // 그래서 serializable 인터페이스 구현 필요. 하지만, 전체 클래스가 serialize 되면 불필요한 정보도 전달 된다.
    // scala는 object로 선언하여 new 키워드 없이 바로 접근 가능하게.
    // java에서는 Function을 구현한 클래스를 드라이버 클래스 내부에서 정의하면 직렬화 문제 발생.
    val rdd2 = rdd1.map(add)
    println(count)
  }

  val increment = 1
  def runMapSample3(sc: SparkContext) = {
    val rdd1 = sc.parallelize(1 to 10)
    // 인스턴스 변수 전달도 직렬화 문제 발생. 지역변수를 활요. (runMapSample4)
    val rdd2 = rdd1.map(_ + increment)
  }

  def runMapSample4(sc: SparkContext) = {
    val rdd1 = sc.parallelize(1 to 10)
    // 인스턴스 변수와 같은 값의 지역 변수를 사용.
    val localIncrement = increment
    val rdd2 = rdd1.map(_ + localIncrement)
  }

}
