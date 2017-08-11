package com.seoeun.ch1;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * To execute,
 * <p>
 *     $ spark-submit --class com.seoeun.ch1.WordCount target/wikibook-0.9-SNAPSHOT.jar local[*] /usr/lib/spark/README.md java-out
 * </p>
 * <p>
 *     $ spark-submit --class com.seoeun.ch1.WordCount --master yarn --deploy-mode client \
 *       deploy/original-wikibook.jar yarn hdfs://sembp:8020/sample/README.md hdfs://sembp:8020/sample/out
 * </p>
 * @author seoeun
 * @since ${VERSION} on 6/5/17
 */
public class WordCount {

    public static void main(String... args) {

        if (ArrayUtils.getLength(args) != 3) {
            System.out.println("Usage: WordCount <master> <input> <output>");
            return;
        }
        String master = args[0];
        String inputFile = args[1];
        String output = args[2];

        JavaSparkContext sc = getSparkContext("WorldCount", master);

        try {
            JavaRDD<String> inputRdd = getInputRdd(sc, inputFile);
            JavaPairRDD<String, Integer> resultRdd = processWithLambda(inputRdd);

            saveResult(resultRdd, output);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.stop();
        }
        
    }

    public static JavaSparkContext getSparkContext(String appName, String master) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        return new JavaSparkContext(conf);
    }

    /**
     * Load from local filesystem.
     * @param sc
     * @param input
     * @return
     */
    public static JavaRDD<String> getInputRdd(JavaSparkContext sc, String input) {
        return sc.textFile(input);
    }

    /**
     * Save to local filesystem.
     * @param resultRdd
     * @param output
     */
    public static void saveResult(JavaPairRDD<String, Integer> resultRdd, String output) {
        System.out.println("---- output = " + output);
        resultRdd.saveAsTextFile(output);
    }

    public static JavaPairRDD<String, Integer> process(JavaRDD<String> inputRdd) {
        JavaRDD<String> words = inputRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> wcPair = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }) ;
        JavaPairRDD<String, Integer> result = wcPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        return result;
    }

    public static JavaPairRDD<String, Integer> processWithLambda(JavaRDD<String> inputRdd) {
        JavaRDD<String> words = inputRdd.flatMap((String s) -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> wcPair = words.mapToPair((String a) -> new Tuple2(a, 1));

        JavaPairRDD<String, Integer> result = wcPair.reduceByKey((Integer c1, Integer c2) -> c1 + c2);

        return result;

    }
    // spark-submit --class com.seoeun.ch1.scala.WordCount1 deploy/wikibook.jar local[*] /usr/lib/spark/README.md ~/spark-out/a


}
