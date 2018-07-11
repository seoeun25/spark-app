from pyspark import SparkConf, SparkContext

# MapPartitions
def increase(numbers):
    print("---Each Partition")
    return (i + 1 for i in numbers)

# MapParitionsWithIndex
def increaseWithIndex(index, numbers):
    print("--- Each Partition")
    for i in numbers:
        if (index == 1):
            yield i


class RDDOpSample:
    def doCollect(self, sc):
        rdd = sc.parallelize(range(1, 11))
        result = rdd.collect()
        print(result)

    def doCount(self, sc):
        rdd = sc.parallelize(range(1, 11))
        result = rdd.count()
        print("count : \n {}".format(result))

    # rdd의 요소에 function을 적용하여 rdd를 만든다.
    def doMap(self, sc):
        rdd = sc.parallelize(range(1, 11))
        result = rdd.map(lambda v: v + 1)
        print("map : \n {}".format(result.collect()))

    # rdd의 요소에 function을 적용하여 iterator를 만들고, 그 iterator를 풀어서 rdd를 만든다.
    def doFlatMap(selfs, sc):
        rdd = sc.parallelize(["apple,orange", "grape,apple,mango", "blueberry,tomato,orange"])
        result = rdd.flatMap(lambda v: v.split(","))
        print("flatMap : \n{}".format(result.collect()))

        rdd2 = rdd.flatMap(lambda v: v.split(",") if v.find("apple") else "")
        print("flatMap 2 : \n{}".format(rdd2.collect()))

    # rdd를 parition 별로 map을 적용: 각 parition에 들어오는 rdd에 function을 적용하여 리턴.
    def doMapPartitions(self, sc):
        rdd1 = sc.parallelize(range(1, 11), 3)
        #rdd2 = rdd1.mapPartitions(increase)
        rdd2 = rdd1.mapPartitions(lambda v: (i + 1 for i in v))
        print("doMapPartitions : \n {}".format(rdd2.collect()))

    # 각 parition에 적용되는 function에 partition index를 함께 전달.
    def doMapParitionsWithIndex(self, sc):
        rdd1 = sc.parallelize(range(1, 11), 3)
        rdd2 = rdd1.mapPartitionsWithIndex(increaseWithIndex)
        print("doMapParitionsWithIndex : \n {}".format(rdd2.collect()))

    # key-value 의 pair 를 key는 그대로 두고, value에 function을 적용하여 pair rdd를 리턴.
    def doMapValues(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"])
        rdd2 = rdd1.map(lambda s: (s, 1))
        print("pairRdd : {}".format(rdd2.collect()))
        rdd3 = rdd2.mapValues(lambda i: i + 1)
        print("mapValues : \n {}".format(rdd3.collect()))

    # key-value pair의 rdd에서 key는 그대로 두고, value에 만 flatmap 적용: value에 function을 적용하여 iterator를 만들고,
    # 그 iterator를 풀어서 다시 key-value로 리턴.
    def doFlatMapValues(self, sc):
        rdd1 = sc.parallelize([(1, "a,b"), (2, "a,c"), (3, "d,e")])
        rdd2 = rdd1.flatMapValues(lambda v: v.split(","))
        print("flatMapValues ; \n {}".format(rdd2.collect()))

    # partition 수도 같고, element 수도 같은 두 rdd를 합쳐서 key-pair rdd로 리턴.
    def doZip(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"])
        rdd2 = sc.parallelize([1, 2, 3])
        rdd3 = rdd1.zip(rdd2)
        print("zip : \n {}".format(rdd3.collect()))

    # rdd의 element에 function을 적용해서 key를 만들고, 그 key에 해당하는 element들을 sequence로 만들어 value로 리턴. (k, sequence)
    def doGroupBy(self, sc):
        rdd1 = sc.parallelize(range(1, 11))
        rdd2 = rdd1.groupBy(lambda v: "even" if v % 2 == 0 else "odd")
        print("groupBy : \n {}".format(rdd2.collect()))
        for x in rdd2.collect():
            print(x[0], list(x[1]))

    # 이미 key-value의 rdd를 key를 기준으로 groupBy 하여 (k, sequence)의 pair rdd로 리턴
    def doGroupByKey(self, sc):
        rdd1 = sc.parallelize(["a", "b", "a", "c", "e"]).map(lambda v: (v, 1))
        rdd2 = rdd1.groupByKey()
        print("groupByKey : \n {}".format(rdd2.collectAsMap()))
        for v in rdd2.collect():
            print(v[0], list(v[1]))

    # 두 개의 key-value pair rdd 를 key를 기준으로 groupby 하여 (k (sequence1, sequence2)) 의 key-value pair로 리턴.
    # sequence1은 rdd1의 element들, sequence2는 rdd2의 element 들.
    def doCogrouop(self, sc):
        rdd1 = sc.parallelize([("k1", "v1"), ("k2", "v2"), ("k1", "v3")])
        rdd2 = sc.parallelize([("k1", "v4")])
        rdd3 = rdd1.cogroup(rdd2)
        print("cogroup : \n {}".format(rdd3.collectAsMap()))
        for x in rdd3.collect():
            print(x[0], list(x[1][0]), list(x[1][1]))

    # rdd의 element의 중복을 제거해서 새로운 rdd를 만들어서 리턴
    def doDistinct(self, sc):
        rdd1 = sc.parallelize([1, 2, 3, 1, 2, 4, 5])
        rdd2 = rdd1.distinct()
        print("distinct : \n {}".format(rdd2.collect()))

    # 두개의 rdd에 대해서 카테시안 곱을 구하고 그 결과를 key-value pair rdd로 리턴
    def doCartesian(self, sc):
        rdd1 = sc.parallelize([1, 2, 3])
        rdd2 = sc.parallelize(["a", "b", "c"])
        rdd3 = rdd1.cartesian(rdd2)
        print("cartesian : \n {}".format(rdd3.collect()))

    # rdd1에는 속하고, rdd2에는 속하지 않는 element들로 rdd를 만들어서 리턴. 빼기.
    def doSubtract(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c", "d", "e"])
        rdd2 = sc.parallelize(["d", "e"])
        rdd3 = rdd1.subtract(rdd2)
        print("subtract : \n {} ".format(rdd3.collect()))

    # rdd1의 요소와 rdd2의 요소를 합쳐서 리턴. 중복되면 중복된 채로 리턴.
    def doUnion(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"])
        rdd2 = sc.parallelize(["c", "d", "e"])
        rdd3 = rdd1.union(rdd2)
        print("union : \n {}".format(rdd3.collect()))

    # rdd1과 rdd2의 교집합
    def doIntersection(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c"])
        rdd2 = sc.parallelize(["b", "c", "d"])
        rdd3 = rdd1.intersection(rdd2)
        print("interscetion : \n {}".format(rdd3.collect()))

    # db의 join과 동작방식이 같음. (k, (v1, v2)) 의 pairRdd를 리턴.
    def doJoin(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c", "d", "e"]).map(lambda v: (v, 1))
        rdd2 = sc.parallelize(["a", "d", "d"]).map(lambda v: (v, 2))
        rdd3 = rdd1.join(rdd2)
        print("join : \n {}".format(rdd3.collect()))
    # db의 outerjoin. (k (v1,v2))의 pair rdd를 리턴.
    def doOuterJoin(self, sc):
        rdd1 = sc.parallelize(["a", "b", "c", "d"]).map(lambda v: (v, 1))
        rdd2 = sc.parallelize([("a", 2), ("c", 3), ("c", 4), ("e", 5)])
        rddLeftOuter = rdd1.leftOuterJoin(rdd2)
        print("leftOuterJoin : \n {}".format(rddLeftOuter.collect()))
        # (a, (1,2)), (b, (1,None)), (c, (1,3)), (c, (1,4), (d, (1, None))
        rddRightOuter = rdd1.rightOuterJoin(rdd2)
        print("rightOuterJoin : \n {}".format(rddRightOuter.collect()))
        # (a, (1,2)) (c, (1,3)) (c, (1,4)) (e, (None,5))



if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.driver.host", "127.0.0.1")
    sc = SparkContext(master="local[*]", appName="RDDOpSample", conf=conf)

    obj = RDDOpSample()
    # obj.doCollect(sc)
    # obj.doCount(sc)
    # obj.doMap(sc)
    # obj.doFlatMap(sc)
    # obj.doMapPartitions(sc)
    # obj.doMapParitionsWithIndex(sc)
    # obj.doMapValues(sc)
    # obj.doFlatMapValues(sc)
    # obj.doZip(sc)
    # obj.doGroupBy(sc)
    # obj.doGroupByKey(sc)
    # obj.doCogrouop(sc)
    # obj.doDistinct(sc)
    # obj.doCartesian(sc)
    # obj.doSubtract(sc)
    # obj.doUnion(sc)
    # obj.doIntersection(sc)
    # obj.doJoin(sc)
    obj.doOuterJoin(sc)

    sc.stop()
