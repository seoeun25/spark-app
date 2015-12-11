package com.seoeun.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * To run, bin/spark-submit --class com.seoeun.spark.WordCount --master local[4] ~/mywork/ws_spark_app/spark-app/example-java/target/example-java-0.9-SNAPSHOT.jar
 *
 */
public class WordCount implements Serializable{


    public WordCount() {

    }

    public static void main(String... args) {
        SparkConf conf = new SparkConf().setAppName("WordCount Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String nameNode = "hdfs://sembp:8020";
        if (args.length > 0) {
            nameNode = args[0];
        }
        WordCount wordCount = new WordCount();
        String inputPath = nameNode + "/user/seoeun/spark/word-input";
        String outputPath = nameNode + "/user/seoeun/spark/word-output";
        wordCount.wordcount(sc, inputPath, outputPath);

        sc.stop();
    }

    public void wordcount(JavaSparkContext sc, String inputPath, String outputPath) {
        JavaRDD<String> file = sc.textFile(inputPath);
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                System.out.println("ssss : " + s);
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        counts.saveAsTextFile(outputPath);
        System.out.println("------------------");
    }
}
