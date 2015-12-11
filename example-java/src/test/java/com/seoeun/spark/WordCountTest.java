package com.seoeun.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

public class WordCountTest implements Serializable {

    private static SparkContext sc;
    private static JavaSparkContext jsc;

    @BeforeClass
    public static void setup() {
        SparkConf conf = new SparkConf().setAppName("Test");
        conf.setMaster("local[2]");
        conf.setAppName("Test");
        sc = new SparkContext(conf);
        jsc = new JavaSparkContext(sc);
    }

    @AfterClass
    public static void cleanup() {
        if (sc != null) {
            jsc.stop();
            sc.stop();
        }
        jsc = null;
        sc = null;
    }

    @Test
    public void test() {
        String namenode = "hdfs://sembp:8020";
        //String srcFile = "/user/seoeun/spark/word-input";
        String srcFile = "/Users/seoeun/libs/spark/README.md";
        JavaRDD<String> file = jsc.textFile("/Users/seoeun/libs/spark/README.md");
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });

        System.out.println(counts.collect());
        //String outputPath = namenode + "/user/seoeun/spark/word-output-test";
        String outputPath = "./target/output-test";
        counts.saveAsTextFile(outputPath);
    }


}
