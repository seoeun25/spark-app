package com.seoeun.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * To run, bin/spark-submit --class com.seoeun.spark.streaming.JavaNetworkWordCount --master local[4] example-java-0.9-SNAPSHOT.jar.
 *
 * $ nc -lk 9999
 * hello world
 *
 */
public class JavaNetworkWordCount implements Serializable{


    public JavaNetworkWordCount() {

    }

    public void readStream(JavaStreamingContext javaStreamingContext) {
        JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream("localhost", 9999);

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        // Count each word in each batch
        JavaPairDStream<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("--------- v1 + v2 : " + (v1.intValue() + v2.intValue()));
                return v1 + v2;
            }
        });

        System.out.println("---- time : " + System.currentTimeMillis() + " ----");
        wordCounts.print();

    }

    public static void main(String... args) {

        String master = "local[2]";
        String appName = "NetworkWordCount";

        SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaNetworkWordCount javaNetworkWordCount = new JavaNetworkWordCount();
        javaNetworkWordCount.readStream(javaStreamingContext);

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }

}
