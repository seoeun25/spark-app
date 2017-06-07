from pyspark import SparkConf, SparkContext


class RDDCreateSample:

    def run(self, sc):

        print("sc = {}".format(sc))

        rdd1 = sc.parallelize(["a", "b", "c", "d", "e"])

        rdd2 = sc.textFile("/usr/lib/spark/README.md")

        print(rdd1.collect())
        print(rdd2.collect())



if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(master="local[*]", appName="RDDCreateSample", conf=conf)

    obj = RDDCreateSample()
    obj.run(sc)

    sc.stop()

