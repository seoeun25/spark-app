package com.seoeun.ch2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * RDD: Resilient Distributed Datasets.
 * 
 * @author seoeun
 * @since ${VERSION} on 6/7/17
 */
public class RDDOpSample {

    public static void main(String... args) throws Exception {
        JavaSparkContext sc = getSparkContext("RDDOpSample", "local[*]");
    }

    /**
     * rdd의 요소를 count. long값을 리턴.
     *
     * @param sc
     */
    public static void doCount(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = parallelizeSampleInt(sc);
        long count = rdd.count();
        System.out.println("count = " + count);
    }

    /**
     * rdd 각 요소에 function을 적용한 값으로 rdd를 생성하여 리턴.
     *
     * @param sc
     */
    public static void doMap(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        JavaRDD<Integer> rdd1 = rdd.map((Integer v1) -> v1 + 1);

        JavaRDD<String> rdd2 = rdd.map((Integer v1) -> String.valueOf(v1) + "a");

        System.out.println(StringUtils.join(rdd2.collect(), ", "));

    }

    /**
     * rdd 요소에 function을 적용하여 iterator를 만들고, 그 iterator를 풀어서 rdd를 만들어서 리턴.
     *
     * @param sc
     */
    public static void doFlatMap(JavaSparkContext sc) {
        JavaRDD<String> fruits = sc.parallelize(Arrays.asList("apple,orange",
                "grape,apple,mango",
                "blueberry,tomato,orange"));
        JavaRDD<String> rdd1 = fruits.map((String s) -> s);
        System.out.println("-- map : \n" + StringUtils.join(rdd1.collect(), "\n"));

        // TODO how to debug
        JavaRDD<String> rdd2 = fruits.flatMap((String s) -> Arrays.asList(s.split(",")).iterator());
//        JavaRDD<String> rdd3 = fruits.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                System.out.println("s = " +s);
//                return Arrays.asList(s.split(",")).iterator();
//            }
//        });
//        List<String> result = rdd2.collect();
//        for (String s: result) {
//            System.out.println("flatmap: " + s);
//        }
        System.out.println("-- flatmap : \n" + StringUtils.join(rdd2.collect(), "\n"));

        // filter
//        JavaRDD<String> rdd3 = fruits.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                if (s.contains("apple")) {
//                    return Arrays.asList(s.split(",")).iterator();
//                } else {
//                    return Arrays.asList("").iterator();
//                }
//            }
//        });
        JavaRDD<String> rdd3 = fruits.flatMap((String s) -> {
            if (s.contains("apple")) {
                return Arrays.asList(s.split(",")).iterator();
            } else {
                return Arrays.asList("").iterator();
            }
        });
        System.out.println("---- flatmap filter(apple) \n" + StringUtils.join(rdd3.collect(), "\n"));
    }

    /**
     * Partition 별로 map을 적용. function의 인자로 partion의 rdd element들의 sequence가 주어지고,
     * 리턴도 각 partition에서 map을 적용한 sequence 를 리턴한 값을 병합한 rdd.
     *
     * @param sc
     */
    public static void doMapPartition(JavaSparkContext sc) {
        // 3 partition
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        JavaRDD<Integer> rdd2 = rdd1.mapPartitions((Iterator<Integer> numbers) -> {
            List<Integer> list = new ArrayList<>();
//            numbers.forEachRemaining(number -> {
//                list.add(number);
//            });
            List<Integer> result = new ArrayList<>();
            numbers.forEachRemaining(result::add);
            System.out.println("--- mapPartitions, numbers = " + Arrays.toString(result.toArray()));

            return result.iterator();
        });
        // rdd2 는 3개의 partition에 작업 결과를 합친 것.
        System.out.println("rdd2 = \n" + rdd2.collect());
    }

    /**
     * partition index 를 함께 인자로 전달하여 map을 실행.
     *
     * @param sc
     */
    public static void doMapPartitionsWithIndex(JavaSparkContext sc) {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        JavaRDD<Integer> rdd3 = rdd1.mapPartitionsWithIndex((Integer index, Iterator<Integer> numbers) -> {
            List<Integer> result = new ArrayList<>();
            if (index == 1) {
                numbers.forEachRemaining(result::add);
            }
            return result.iterator();
        }, true);
        System.out.println(rdd3.collect());

    }

    /**
     * key, value 의 PairRDD에서 사용.
     * key는 그대로 두고, rdd 요소의 value에 function을 적용하여 RDD를 만들어 리턴. (k, v) pair.
     *
     * @param sc
     */
    public static void doMapValues(JavaSparkContext sc) {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
        // key, value PaiRDD
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> rdd3 = rdd2.mapValues(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 + 1;
            }
        });
        System.out.println("old style = \n" + rdd3.collect());
        //List<Tuple2<String,Integer> list = rdd3.collect();

        JavaPairRDD<String, Integer> rdd4 = rdd1.mapToPair((String s) -> new Tuple2<>(s, 1))
                .mapValues((Integer v1) -> v1 + 1);
        System.out.println("java8 style = \n" + rdd4.collect());

    }

    /**
     * Key, Value pair에만 사용. Key는 그대로 두고, values만 flatmap 한 후, key-value로 만들어서 리턴.
     *
     * @param sc
     */
    public static void doFlatMapValues(JavaSparkContext sc) {
        List<Tuple2<Integer, String>> data = Arrays.asList(new Tuple2<>(1, "a,b"),
                new Tuple2<>(2, "c,d"),
                new Tuple2<>(3, "a,d"),
                new Tuple2<>(1, "e,f"));
        JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(data);

        JavaPairRDD<Integer, String> rdd2 = rdd1.flatMapValues((String s) -> Arrays.asList(s.split(",")));
        System.out.println("flatMapValues : \n" + rdd2.collect());
        // (1, a), (1, b), (2, c), (2, d) ...
        Map<Integer, Long> a = rdd2.countByKey();
        a.forEach((Integer i, Long l) -> System.out.println(i + " = " + l));
    }

    // GROUP 관련

    /**
     * Partition  갯수도 같고, element 갯수도 같은 rdd 를 합쳐서 key-value 로 리턴.
     *
     * @param sc
     */
    public static void doZip(JavaSparkContext sc) {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3));

        JavaPairRDD<String, Integer> rdd3 = rdd1.zip(rdd2);
        System.out.println("-- zip \n" + rdd3.collect());
    }

    /**
     * Partition 갯수가 같을 때, 두개의 rdd를 합쳐서 하나의 value로 리턴.
     *
     * @param sc
     */
    public static void doZipPartitions(JavaSparkContext sc) {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"), 3);
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3), 3);

        JavaRDD<String> rdd3 = rdd1.zipPartitions(rdd2, new FlatMapFunction2<Iterator<String>, Iterator<Integer>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> t1, Iterator<Integer> t2) throws Exception {
                List<String> list = new ArrayList<>();
                while (t1.hasNext()) {
                    while (t2.hasNext()) {
                        list.add(t1.next() + t2.next());
                    }
                }
                return list.iterator();
            }
        });

        JavaRDD<String> rdd4 = rdd1.zipPartitions(rdd2, (Iterator<String> t1, Iterator<Integer> t2) -> {
            List<String> list = new ArrayList<>();
            t1.forEachRemaining((String s) -> {
                t2.forEachRemaining((Integer i) -> {
                    list.add(s + i);
                });
            });
            return list.iterator();
        });

        System.out.println("zipPartition : \n" + rdd3.collect());
        System.out.println("zipPartition : \n" + rdd4.collect());

    }

    /**
     * rdd의 element에 Function을 적용하여 key를 만들고, 그 key를 기준으로 groupBy한 결과를 iterable에 넣어서, key-value로 리턴.
     * (k, sequence)
     *
     * @param sc
     */
    public static void doGroupBy(JavaSparkContext sc) {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupBy(new Function<Integer, String>() {
            @Override
            public String call(Integer v1) throws Exception {
                return v1 % 2 == 0 ? "event" : "odd";
            }
        });
        JavaPairRDD<String, Iterable<Integer>> rdd3 =
                rdd1.groupBy((Integer i) -> (i % 2 == 0 ? "event" : "odd"));
        System.out.println("groupBy : \n" + rdd2.collect());
        System.out.println("groupBy : \n" + rdd3.collect());
    }

    /**
     * 이미 key-value PairRDD를 key를 기준으로 groupBy 해서 key-value(Iterable)로 리턴.
     *
     * @param sc
     */
    public static void doGroupByKey(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2<>("a", 1),
                new Tuple2<>("b", 1),
                new Tuple2<>("c", 1),
                new Tuple2<>("b", 1),
                new Tuple2<>("c", 1));
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data);
        JavaPairRDD<String, Iterable<Integer>> rdd3 = rdd1.groupByKey();
        System.out.println("groupByKey : \n" + rdd3.collect());
    }

    /**
     * key-value의 rdd 2개를 그룹핑하여, key-value로 리턴.
     * 리턴은 key-value(<iterable 1,iterable 2>) 인데, key는 두 rdd의 모든 key이고,
     * iterable 1에는 rdd1의 value들, iterable 2에는 rdd2의 value들이 들어 있다.
     * (k, (sequence1, sequence2))
     *
     * @param sc
     */
    public static void doCoGroupBy(JavaSparkContext sc) {
        List<Tuple2<String, String>> data1 = Arrays.asList(new Tuple2<>("a", "a-1"),
                new Tuple2<>("a", "a-2"),
                new Tuple2<>("b", "b-1"));
        List<Tuple2<String, String>> data2 = Arrays.asList(new Tuple2<>("c", "c-1"),
                new Tuple2<>("a", "a-1"),
                new Tuple2<>("e", "e-1"));
        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(data1);
        JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(data2);

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> result
                = rdd1.cogroup(rdd2);

        System.out.println(result.collect());

    }

    // 집합 관련

    /**
     * rdd의 element의 중복을 제거한 rdd를 리턴
     *
     * @param sc
     */
    public static void doDistinct(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 4, 4, 5));
        JavaRDD<Integer> result = rdd.distinct();
        System.out.println(result.collect());
    }

    /**
     * 2개의 rdd를 cartesian 곱을 구하고 그 값을 key-value로 리턴. 곱하기.
     *
     * @param sc
     */
    public static void doCartesian(JavaSparkContext sc) {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b", "c"));
        JavaPairRDD<Integer, String> result = rdd1.cartesian(rdd2);

        System.out.println(result.collect());
    }

    /**
     * rdd1에는 속하고, rdd2에는 속하지 않는 요소들을 리턴. 빼기.
     *
     * @param sc
     */
    public static void doSubtract(JavaSparkContext sc) {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b"));
        JavaRDD<String> result = rdd1.subtract(rdd2);

        System.out.println(result.collect());
    }

    /**
     * rdd1에 속하거나 rdd2에 속하는 요소들을 리턴. 중복되면 중복된 채로 리턴. 모두 그냥 포함하기. 합집합.
     *
     * @param sc
     */
    public static void doUnion(JavaSparkContext sc) {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("c", "d", "e"));
        JavaRDD<String> result = rdd1.union(rdd2);

        System.out.println(result.collect());
    }

    /**
     * rdd1 과 rdd2에 동시에 속하는 요소들을 리턴. 교집합.
     *
     * @param sc
     */
    public static void doIntersection(JavaSparkContext sc) {
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "c"));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("c", "d", "e"));
        JavaRDD<String> result = rdd1.intersection(rdd2);

        System.out.println(result.collect());
    }

    // Join

    /**
     * key-value인 두 개의 rdd를 join. DB의 join과 유사한 동작.
     * rdd1에 있는 key를 기준으로 rdd2를 join 하여 (k (v1, v2)) 튜플들 리턴.
     *
     * @param sc
     */
    public static void doJoin(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> data1 = Arrays.asList(new Tuple2<>("a", 1),
                new Tuple2<>("b", 1),
                new Tuple2<>("c", 1),
                new Tuple2<>("d", 1),
                new Tuple2<>("c", 2));
        List<Tuple2<String, Integer>> data2 = Arrays.asList(new Tuple2<>("a", 3),
                new Tuple2<>("c", 4),
                new Tuple2<>("c", 6));
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);

        JavaPairRDD<String, Tuple2<Integer, Integer>> result = rdd1.join(rdd2);

        System.out.println("join : \n" + result.collect());
    }

    /**
     * key-value 의 두 rdd를 outer join. DB의 outer join과 같은 원리.
     * (k, (v1, v2)) 의 tuple 들 리턴.
     *
     * @param sc
     */
    public static void doOuterJoin(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> data1 = Arrays.asList(new Tuple2<>("a", 1),
                new Tuple2<>("b", 1),
                new Tuple2<>("c", 1),
                new Tuple2<>("c", 2));
        List<Tuple2<String, Integer>> data2 = Arrays.asList(new Tuple2<>("a", 2),
                new Tuple2<>("c", 3),
                new Tuple2<>("d", 4));
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);

        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> leftOuterJoin = rdd1.leftOuterJoin(rdd2);
        System.out.println("leftOuterJoin : \n" + leftOuterJoin.collect());

        JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> rightOuterJoin = rdd1.rightOuterJoin(rdd2);
        System.out.println("rightOuterJoin : \n" + rightOuterJoin.collect());

    }

    /**
     * 2개의 pair rdd. key를 기준으로 rdd1에서 rdd2를 뺀 pair rdd를 리턴.
     *
     * @param sc
     */
    public static void doSubtractByKey(JavaSparkContext sc) {
        JavaPairRDD<String, Integer> rdd1 = sc.parallelize(Arrays.asList("a", "b"))
                .mapToPair((String v) -> new Tuple2<>(v, 1));
        JavaPairRDD<String, Integer> rdd2 = sc.parallelize(Arrays.asList("b"))
                .mapToPair((String s) -> new Tuple2<>(s, 2));
        JavaPairRDD<String, Integer> rdd3 = rdd1.subtractByKey(rdd2);
        System.out.println("subtractByKey : \n" + rdd3.collect());
    }

    // 집계 관련

    /**
     * 1개의 pair rdd를 key를 기준으로 value값을 병합하는 function을 실행하여 리턴. (k, v)
     * 병합하는 function의 인자는 v1, v2 두개로, 두 값을 연산하는 function은 결합법칙, 교환법칙이 성립되어야 한다.
     * 왜냐하면, 데이터가 여러 파티션에 분산되어 있어서 항상 같은 순서로 연산될 것을 보장하지 않는다.
     * partition에 각 key별로 function을 적용하고, partition을 합치면서 function을 다시 적용.
     *
     * @param sc
     */
    public static void doReduceByKey(JavaSparkContext sc) {
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("a", 1),
                new Tuple2<>("b", 1), new Tuple2<>("a", 3),
                new Tuple2<>("a", 2), new Tuple2<>("c", 3)), 2);
        //JavaPairRDD<String, Integer> rdd2 = rdd1.reduceByKey((Integer v1, Integer v2) -> v1 + v2);
        JavaPairRDD<String, Integer> rdd3 = rdd1.reduceByKey((Integer v1, Integer v2) -> {
            System.out.println(String.format("v1 = %s, v2 = %s ", v1, v2));
            return v1 + v2;
        });
        System.out.println("reduceByKey : \n" + rdd3.collect());
        // (a, 6), (b, 1) (c, 1)
    }

    /**
     * reduceByKey와 비슷하게 동작. 병합을 실행하는 function과 초기값을 인자로 받아서 function을 실행하여 (k,v)를 리턴
     * partition에 각 key별로 function을 적용하고, partition을 합치면서 function을 다시 적용.
     *
     * @param sc
     */
    public static void doFoldByKey(JavaSparkContext sc) {
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("a", 2),
                new Tuple2<>("b", 5), new Tuple2<>("a", 3),
                new Tuple2<>("a", 4), new Tuple2<>("c", 4)), 2);
        JavaPairRDD<String, Integer> rdd2 = rdd1.foldByKey(10, (Integer v1, Integer v2) -> v1 + v2);
        JavaPairRDD<String, Integer> rdd3 = rdd1.foldByKey(10, (Integer v1, Integer v2) -> {
            System.out.println(String.format("v1 = %s, v2 = %s, r = %s ", v1, v2, (v1 * v2)));
            return v1 * v2;
        });
        // (a, 2400), (b, 50), (c, 10)
        System.out.println("foldByKey : \n" + rdd3.collect());
    }

    /**
     * pair rdd 를 key 별로 combiner function을 적용하여 만들어진 새로운 타입의 객체(C)를 pair rdd로 리턴. (k, c)
     * combiner function은 병합을 구현하여 새로운 타입의 개체를 만들어야 한다.
     * 다음의 3개 function이 필요
     * createCombiner() : 병합을 구현하는 객체 생성.
     * mergeValue(): combiner가 있으면 기존의 combiner 함수를 사용해서 데이터를 병합. 각 key별로 데이터가 병합된다.
     * mergeCombiner(): 각 파티션에서 만들어진 combiner를 다시 병합하여 최종의 데이터를 생성.
     * 즉, 각 파티션에서 key별로 createCombiner(), mergeValue()를 실행하여 데이터를 병합하고,
     * 마지막에는 각 파티션에서 만든 combiner를 mergeCombiner()를 실행하여 병합한다.
     *
     * @param sc
     */
    public static void doCombineByKey(JavaSparkContext sc) {
        JavaPairRDD<String, Long> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("Math", 100L),
                new Tuple2<>("Eng", 80L),
                new Tuple2<>("Math", 50L),
                new Tuple2<>("Eng", 60L),
                new Tuple2<>("Eng", 90L)), 2);

        Function<Long, Record> createCombiner = new Function<Long, Record>() {
            @Override
            public Record call(Long v1) throws Exception {
                System.out.println("createCombiner. v1 = " + v1);
                return new Record(v1);
            }
        };
        JavaPairRDD<String, Record> rdd2 = rdd1.combineByKey(createCombiner, new Function2<Record, Long, Record>() {
            @Override
            public Record call(Record record, Long v2) throws Exception {
                System.out.println("mergeValue. record = " + record.toString() + ", v2 = " + v2);

                return record.add(v2);
            }
        }, new Function2<Record, Record, Record>() {
            @Override
            public Record call(Record v1, Record v2) throws Exception {
                System.out.println("mergeCombiner. record1 = " + v1.toString() + ", v2 = " + v2.toString());
                return v1.add(v2);
            }
        });
        System.out.println("combinByKey : \n" + rdd2.collect());

        JavaPairRDD<String, Record> rdd3 = rdd1.combineByKey((Long v1) -> new Record(v1),
                (Record r1, Long v2) -> r1.add(v2),
                (Record r1, Record r2) -> r1.add(r2));

        //System.out.println("combineByKey : \n" + rdd3.collect());
        // (Math, (Record=150,2,75)), (Eng, (Record=230,3,76))
    }

    /**
     * combineByKey와 동작이 같으나 combiner를 생성할 때 초기값이 주어진다.
     * 각 파티션마다 key 별로.
     *
     * @param sc
     */
    public static void doAggregateByKey(JavaSparkContext sc) {
        JavaPairRDD<String, Long> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("Math", 100L),
                new Tuple2<>("Eng", 80L),
                new Tuple2<>("Math", 50L),
                new Tuple2<>("Eng", 60L),
                new Tuple2<>("Eng", 90L)), 3);

        JavaPairRDD<String, Record> rdd2 = rdd1.aggregateByKey(new Record(100, 0),
                (Record r1, Long v1) -> r1.add(v1),
                (Record r1, Record r2) -> r1.add(r2));
        System.out.println("aggregateByKey : \n" + rdd2.collect());
        // (Math (350, 2, 175)) (Eng (

    }

    // pipe 및 파티션

    /**
     * 외부 커맨드를 이용하여 데이터를 가져온다.
     * linux의 cut 유틸을 사용하는 예.
     *
     * @param sc
     */
    public static void doPipe(JavaSparkContext sc) {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("1,2,3", "4,5,6", "7,8,9"));
        JavaRDD<String> rdd2 = rdd.pipe("cut -f 1,3 -d ,");
        System.out.println("pipe : \n" + rdd2.collect());
    }

    /**
     * Partition 갯수 조정. coalesce는 줄이는 것만. repartition은 줄이고 늘이고.
     * coalesce 를 사용하면 셔플 옵션을 주지 않으면 셔플하지 않아서 성능이 좋다.
     *
     * @param sc
     */
    public static void doPartition(JavaSparkContext sc) {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 10);
        JavaRDD<Integer> rdd2 = rdd1.coalesce(5);
        JavaRDD<Integer> rdd3 = rdd2.coalesce(10);
        System.out.println("partition size. rdd1 = " + rdd1.getNumPartitions());
        System.out.println("partition size. rdd2 = " + rdd2.getNumPartitions());
        System.out.println("partition size. rdd3 = " + rdd3.getNumPartitions());

        JavaRDD<Integer> rdd4 = rdd2.repartition(10);
        System.out.println("partition size. rdd4 = " + rdd4.getNumPartitions());
    }

    /**
     * Pair rdd의 key를 partitioner를 적용하여 각 partition으로 이동시키고 각 partition에서 key에 따라 정렬시켜서 pair rdd 로 리턴.
     *
     * @param sc
     */
    public static void doRepartitionAndSortWithinPartitions(JavaSparkContext sc) {
        Random random = new Random();
        List<Integer> data = random.ints(10, 0, 100)
                .boxed().collect(Collectors.toList());
        JavaPairRDD<Integer, String> rdd = sc.parallelize(data).mapToPair((Integer i) -> new Tuple2<>(i, "-"));
        JavaPairRDD<Integer, String> rdd2 = rdd.repartitionAndSortWithinPartitions(new HashPartitioner(3));
        // data 검증
        rdd2.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, String>>>() {
            @Override
            public void call(Iterator<Tuple2<Integer, String>> tuple2Iterator) throws Exception {
                System.out.println(Thread.currentThread().getName() + " ======");
                List<Tuple2<Integer, String>> list = new ArrayList<>();
                tuple2Iterator.forEachRemaining(list::add);  // 정열됨.
                System.out.println(Arrays.toString(list.toArray()));
                System.out.println(Thread.currentThread().getName() + " :  end");
            }
        });
    }

    /**
     * rdd에 partitioner 적용. {@linkplain HashPartitioner} or {@linkplain org.apache.spark.RangePartitioner}
     *
     * @param sc
     */
    public static void doPartitionBy(JavaSparkContext sc) {
        Random random = new Random();
        List<Integer> data = random.ints(10, 0, 100)
                .boxed().collect(Collectors.toList());
        JavaPairRDD<Integer, String> rdd = sc.parallelize(data).mapToPair((Integer i) -> new Tuple2<>(i, "-"));
        JavaPairRDD<Integer, String> rdd2 = rdd.partitionBy(new HashPartitioner(3));

        System.out.println("partitions = " + rdd2.getNumPartitions());
    }

    // filter

    /**
     * Filter를 구현한 function을 적용하여 filtering한 RDD를 리턴.
     *
     * @param sc
     */
    public static void doFilter(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> rdd2 = rdd.filter((Integer i) -> i > 2);
        System.out.println("filter : \n" + rdd2.collect());
    }

    /**
     * pair rdd를 key를 기준으로 sort. pair rdd 를 리턴.
     *
     * @param sc
     */
    public static void doSortByKey(JavaSparkContext sc) {
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(new Tuple2<>("q", 2),
                new Tuple2<>("z", 1), new Tuple2<>("a", 3)));
        JavaPairRDD<String, Integer> rdd2 = rdd.sortByKey();
        System.out.println("sortByKey : \n" + rdd2.collect());
    }

    /**
     * pair rddd의 key들, value를만 뽑아서 rdd로 리턴.
     *
     * @param sc
     */
    public static void doKeysValues(JavaSparkContext sc) {
        JavaPairRDD<String, String> rdd = sc.parallelizePairs(Arrays.asList(new Tuple2<>("k1", "v1"),
                new Tuple2<>("k2", "v2"), new Tuple2<>("k3", "v3")));
        System.out.println("keys : \n" + rdd.keys().collect());
        System.out.println("values : \n" + rdd.values().collect());

    }

    // Actions : 리턴 값이 rdd가 아닌 다른 타입.
    // lazy evaluation: 메서드를 호출 하는 시점에 실행되는 것이 아니라, 실제로 계산이 필요한 시점에서 실행.
    // rdd transformation, action모두 lazy evaluation 방식.
    // rdd transformation 을 호출하고, 예를 들어 map을 10번 실행하고 count()를 실행하면 count같은 액션 메서드가 호출 될 때 map이 10번 실행된다.
    // 액션을 여러번 호출하면 그때마다 다시 transformation을 실행.

    public static void doFrist(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(5, 4, 6));
        Integer first = rdd.first();
        System.out.println("first : " + first);

        List<Integer> takes = rdd.take(3);
        System.out.println("takes : \n" + Arrays.toString(takes.toArray()));

        long count = rdd.count();
        System.out.println("count : " + count);
    }

    /**
     * rdd요소들의 값이 나타는 회수를 구해서 map으로 리턴.
     *
     * @param sc
     */
    public static void doCountByValue(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 2, 3));
        Map<Integer, Long> map = rdd.countByValue();
        System.out.println(map);
        // (1=2, 2=2, 3=1)
    }

    /**
     * 각 요소에 적용할 function을 구현,  function을 적용한 값이 리턴된다.
     * function은 각 partition에서 구현되기 때문에 입력되는 parameter가 순서대로 오지 않을 수
     * 있다.
     * 따라서 각 요소에 대한 교환 법칙, 결합 법칙이 성립 되어야 한다.
     * reduceByKey와 다른 점은 key를 기준으로 각 요소에 적용하는 냐, key가 없으니 각 partition에서 요소들 전체에 적용하느냐.
     *
     * @param sc
     */
    public static void doReduce(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);

        Integer reduce1 = rdd.reduce((Integer v1, Integer v2) -> v1 + v2);
        System.out.println("reduce = " + reduce1);

        Integer reduce2 = rdd.reduce((Integer v1, Integer v2) -> {
            System.out.println(String.format("v1 = %s, v2 = %s", v1, v2));
            return v1 + v2;
        });
        System.out.println("reduce2 = " + reduce2);
        // 55
    }

    /**
     * function을 구현하고, function 을 적용할 때 초기값을 인자로 주어 적용.
     * 각 partition마다 초기값을 적용한 function이 먼저 실행된다.
     * function의 결과 값을 리턴.
     * 같은 방법으로 여러번 실행되도 같은 값을 리턴할 수 있는 초기값을 셋팅. 예를 들면 덧셈음 0, 곱셈은 1.
     * 각 파티션마다 초기값이 적용되고, partition을 합칠 때도 초기 값이 적용된다.
     *
     * @param sc
     */
    public static void doFold(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);

        Integer fold2 = rdd.fold(10, (Integer v1, Integer v2) -> {
            System.out.println("v1 = " + v1 + ", v2 = " + v2);
            return v1 + v2;
        });

        System.out.println("fold2 = " + fold2);
        // 95
    }

    /**
     * 다음의 3개 인자를 필요.
     * 1. initialValue : 초기값. 각 파티션에 처음 seqOp을 적용할 때 적용.
     * 2. seqOp : 각 파티션에서 적용할 function. 파티션에 유입되는 element들을 function에 적용하여 연산
     * 3. combOp : 각 파티션의 결과 값을 병할 할 때 적용하는 function.
     * seqOp과 combOp을 적용하여 rdd 요소와는 다른 type의 객체를 반환.
     *
     * @param sc
     */
    public static void doAggregate(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(100, 80, 75, 90, 95), 3);
        Record zeroValue = new Record(0, 0);

        Function2<Record, Integer, Record> seqOp1 = new Function2<Record, Integer, Record>() {
            @Override
            public Record call(Record v1, Integer v2) throws Exception {
                System.out.println("-- seqOp. v1 = " + v1 + " v2 = " + v2);
                return v1.add(v2);
            }
        };
        Function2<Record, Record, Record> combOp1 = new Function2<Record, Record, Record>() {
            @Override
            public Record call(Record v1, Record v2) throws Exception {
                System.out.println("-- combOp. v1 = " + v1 + ", v2 = " + v2);
                return v1.add(v2);
            }
        };

        Record result = rdd.aggregate(zeroValue, seqOp1, combOp1);
        System.out.println("aggregate : \n" + result.toString());
        // amount =  440 (100 + 80 + 77 + 90 + 95), number = 5, avg = 88

        Function2<Record, Integer, Record> seqOp2 = (Record r1, Integer i) -> r1.add(i);
        Function2<Record, Record, Record> combOp2 = (Record r1, Record r2) -> r1.add(r2);
        Record result2 = rdd.aggregate(zeroValue, seqOp2, combOp2);
        System.out.println("aggregate 2 : \n" + result2.toString());

        Record result3 = rdd.aggregate(zeroValue, (Record r1, Integer i) -> r1.add(i), (Record r1, Record r2) ->
                r1.add(r2));
        System.out.println("aggregate 3 : \n" + result3.toString());

    }

    /**
     * double 타입의 element를 가진 rdd에 적용
     *
     * @param sc
     */
    public static void doSum(JavaSparkContext sc) {
        JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d));
        double result = rdd.sum();
        System.out.println("sum : " + result);
    }

    /**
     * rdd의 요소에 function을 적용. 리턴 값은 없음. 각 노드에서 실행됨.
     * foreachPartition은 partition단위로 function을 적용. 즉, partition에 해당되는 sequence를 인자로 받아서 function을 실행.
     *
     * @param sc
     */
    public static void doForEach(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        rdd.foreach((Integer i) -> System.out.println("Value Side Effect: " + i));

        rdd.foreachPartition((Iterator<Integer> iter) -> {
            System.out.println("Partition Side Effect!!");
            while (iter.hasNext()) {
                System.out.println("Value Side Effect : " + iter.next());
            }
        });
    }

    /**
     * rdd의 debugString을 리턴.
     *
     * @param sc
     */
    public static void doDebugString(JavaSparkContext sc) {
        JavaRDD<String> result = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 3).map((Integer i) -> i + "a");
        System.out.println("doDebugString : \n" + result.toDebugString());
    }

    /**
     * rdd cache: 메모리에 저장.
     * rdd persist : 옵션에 따라 memory_only, disk_only, memory_disk 등. serialize 옵션.
     * rdd unpersist : cache 한 것을 해제.
     *
     * @param sc
     */
    public static void doCache(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        rdd.cache();
        rdd.persist(StorageLevel.MEMORY_ONLY());

        //rdd.unpersist();
    }

    /**
     * rdd partition 정보 얻는 법
     *
     * @param sc
     */
    public static void doPartitions(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        System.out.println("partition infos size : " + rdd.partitions().size());
        System.out.println("partition numPartitions : " + rdd.getNumPartitions());
    }

    public static void saveAndLoadTextFile(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(fillToN(1000), 3);
        Class codec = GzipCodec.class;

        // save
        rdd.saveAsTextFile("./sub1");

        // save gzip
        rdd.saveAsTextFile("./sub2", codec);

        // load
        JavaRDD<String> rdd2 = sc.textFile("./sub1");
        System.out.println("load = " + rdd2.count());

        JavaRDD<String> filtered = rdd2.filter((String s) -> Integer.parseInt(s) % 2 == 0);
        System.out.println("filtered = " + filtered.count());

        JavaRDD<String> filtered2 = rdd2.filter((String s) -> {
            return !s.equals("0") && Integer.parseInt(s) % 2 == 0;
        });
        System.out.println("filtered2 = " + filtered2.count());

        JavaPairRDD<String, Iterable<String>> filtered3 = rdd2.groupBy((String s) -> Integer.parseInt(s) % 2 == 0 ?
                "even" : "odd");
        System.out.println("---- collect = \n " + filtered3.collect());

        JavaPairRDD<String, String> mapToParied2 = rdd2.mapToPair((String s) -> new Tuple2<>(
                Integer.parseInt(s) % 2 == 0 ? "even" : "odd", s
        ));

        Map<String, Long> counts = mapToParied2.countByKey();
        counts.forEach((String s, Long i) -> System.out.println(s + " = " + i));
        // even=500, odd=500
    }

    public static void saveAndLoadObjectFile(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(fillToN(1000), 3);
        // save
        rdd.saveAsObjectFile("./sub_object");

        // load
        JavaRDD<Integer> loaded = sc.objectFile("./sub_object");

        JavaPairRDD<String, Integer> mapToPaired = loaded.mapToPair((Integer i) -> new Tuple2<>(
                i % 2 == 0 ? "even" : "odd", i
        ));
        Map<String, Long> grouped = mapToPaired.countByKey();
        grouped.forEach((String s, Long l) -> System.out.println(s + " = " + l));
        // even=500, odd = 500
    }

    /**
     * Hadoop의 sequence file로 저장.
     * Sequence file format은 key와 value로 구성된 데이터를 binary 로 저장하는 파일 포맷.
     * Object file로 저장하는 것은 Java의 직렬화를 사용하기 때문에 serialize 인터페이스를 구현한 객체만 가능.
     * Sequence file로 저장하는 것은 Hadoop의 직렬화 프레임워크를 사용하기 때문에 Hadoop의 Writable을 구현한 객체만 가능.
     *
     * @param sc
     */
    public static void saveAndLoadSequenceFile(JavaSparkContext sc) {
        JavaPairRDD<Text, IntWritable> rdd = sc.parallelize(fillToN(1000), 3).mapToPair((Integer i) ->
                new Tuple2<>(new Text(i % 2 == 0 ? "even" : "odd"), new IntWritable(i)));

        // save
        rdd.saveAsNewAPIHadoopFile("./sub_sequence", Text.class, IntWritable.class, SequenceFileOutputFormat.class);

        // load
        JavaPairRDD<Text, IntWritable> loaded = sc.newAPIHadoopFile("./sub_sequence", SequenceFileInputFormat.class,
                Text.class, IntWritable.class, new Configuration());

        JavaRDD<Integer> rdd2 = loaded.map((Tuple2<Text, IntWritable> tuple) -> tuple._2().get());
        long count = rdd2.count();

        System.out.println("count = " + count);
        // count = 1000

    }

    /**
     * cluster 간에 변수 공유. 읽기 공유.
     * 동일한 stage내에서 실행되는 태스크간에는 자동으로 브로드캐스트 변수를 이용해서 전달.
     * 여러 스테이지에서 반복적으로 활용되는 경우, 명시적으로 브로드캐스트 변수 사용.
     *
     * @param sc
     */
    public static void doBroadCast(JavaSparkContext sc) {
        Broadcast<Set<String>> bu = sc.broadcast(new HashSet<>(Arrays.asList("u1", "u2")));

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("u1", "u3", "u3", "u4", "u5", "u6"), 3);

        // Read from broadcast and filter
        JavaRDD<String> result = rdd.filter((String s) -> bu.value().contains(s));
        System.out.println("filtered using broadcast = \n" + result.collect());
        // u1
    }

    /**
     * cluster간에 공유하여 읽고 쓸 수 있는 accumulator를 이용하여 data validation check.
     *
     * @param sc
     */
    public static void doAccumulator(JavaSparkContext sc) {
        LongAccumulator invalidFormat = sc.sc().longAccumulator("invalidFormat");
        CollectionAccumulator invalidFormat2 = sc.sc().collectionAccumulator("invalidFormat2");

        List<String> data = Arrays.asList("U1:Addr1", "U2:Addr2", "U3", "U4:Addr4", "U5;Addr5", "U6::Addr6", "U7");
        sc.parallelize(data, 3).foreach((String s) -> {
            if (s.split(":").length != 2) {
                invalidFormat.add(1L);
                invalidFormat2.add(s);
            }
        });

        System.out.println("invalidFormat count = " + invalidFormat.value());
        // 4
        System.out.println("invalidFormat values = " + invalidFormat2.value());
        // U3, U5;Addr5, U6::Addr6, U7
    }


    public static ArrayList<Integer> fillToN(int n) {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(i);
        }
        return list;
    }

    public static List<Integer> fillToNRandom(int n) {
        Random random = new Random();
        return random.ints(n, 0, 100).boxed().collect(Collectors.toList());
    }

    private static JavaRDD<Integer> parallelizeSampleInt(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        return rdd;
    }

    public static JavaSparkContext getSparkContext(String appName, String master) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        return new JavaSparkContext(conf);
    }
}
