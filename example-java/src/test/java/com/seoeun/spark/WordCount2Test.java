package com.seoeun.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

public class WordCount2Test implements Serializable {

    private static SparkContext sc;
    private static JavaSparkContext jsc;
    private static WordCount wordCountApp;

    @BeforeClass
    public static void setup() {
        SparkConf conf = new SparkConf().setAppName("Test");
        conf.setMaster("local[2]");
        conf.setAppName("Test");
        sc = new SparkContext(conf);
        jsc = new JavaSparkContext(sc);
        wordCountApp = new WordCount();
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
        String inputPath = "/Users/seoeun/libs/spark/README.md";
        String outputPath = "file:///Users/seoeun/output-test";
        wordCountApp.wordcount(jsc, inputPath, outputPath);
    }

}
