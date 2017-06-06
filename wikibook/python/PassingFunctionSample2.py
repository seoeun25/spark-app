# class 내부가 아닌 최상위에 위치. 또는 지역 함수를 선언해서 사용. 맴버 변수를 사용하면 serialize 문제 발생.
from pyspark import SparkConf, SparkContext


class PassingFunctionSample2():
    def add1(self, i):
        return i + 1
    def runMapSample1(self, sc):
        rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        # rdd2 = rdd1.map(self.add1) // 잘못된 방법. self 를 전달하면 안된다.
        rdd2 = rdd1.map(add2)
        print(", ".join(str(i) for i in rdd2.collect()))


if __name__ == "__main__":
    def add2(i):
        return i + 1
    conf = SparkConf()
    sc = SparkContext(master="local", appName="PassingFunctionSample", conf=conf)
    obj = PassingFunctionSample2()
    obj.runMapSample1(sc)
    sc.stop()
