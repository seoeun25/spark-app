package com.seoeun.ch1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author seoeun
 * @since ${VERSION} on 6/5/17
 */
public class WordCountTest {

    private static SparkConf conf;
    private static JavaSparkContext sc;

    @BeforeClass
    public static void setupClass() {
        conf = new SparkConf().setAppName("WordCountTest").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
    }

    @AfterClass
    public static void cleanup() {
        if (sc != null) {
            sc.stop();
        }
    }

    @Test
    public void testProcess() {

        List<String> input = new ArrayList<>();
        input.add("Apache Spark is a fast and general engine for large-scale data processing.");
        input.add("Spark runs on both Windows and UNIX-like systems");

        JavaRDD<String> inputRdd = sc.parallelize(input);
        JavaPairRDD<String, Integer> resultRdd = WordCount.processWithLambda(inputRdd);

        Map<String, Integer> resultMap = resultRdd.collectAsMap();
        for (Map.Entry<String, Integer> entry: resultMap.entrySet()) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }
        Assert.assertEquals(2, resultMap.get("Spark").intValue());
        Assert.assertEquals(2, resultMap.get("and").intValue());
        Assert.assertEquals(1, resultMap.get("runs").intValue());
        Assert.assertEquals(Optional.empty(), java.util.Optional.ofNullable(resultMap.get("seoeun")));

    }


}
