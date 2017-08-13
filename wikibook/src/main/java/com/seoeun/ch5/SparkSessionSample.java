package com.seoeun.ch5;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

import static org.apache.spark.sql.functions.*;



/**
 * @author seoeun
 * @since ${VERSION} on 8/12/17
 */
public class SparkSessionSample {

    public static void main(String... args) {
        SparkSession spark = SparkSession.builder().appName("Sample").master("local[*]").getOrCreate();

        String source = "file:///usr/lib/spark/README.md";
        Dataset<Row> df = spark.read().text(source);

        runUntypedTransformationsExample(spark, df);

        runTypedTransformationsExample(spark, df);

        runTypedTransformationsExample2(spark, df);


    }

    /**
     * Untyped Operation을 사용. (Row, Column을 사용)
     * 비타입데이터는 RDD와는 다르게 Row와 Column 단위로 데이터를 처리.
     * @param spark
     * @param df
     */
    public static void runUntypedTransformationsExample(SparkSession spark, Dataset<Row> df) {
        Dataset<Row> wordDF = df.select(explode(split(col("value"), " ")).as("word"));
        Dataset<Row> result = wordDF.groupBy("word").count();
        result.show();
    }

    public static void runTypedTransformationsExample(SparkSession spark, Dataset<Row> df) {
        // df는 Row 타입이기 때문에 원래의 타입인 String으로 변환하여 Dataset 생성.
        Dataset<String> ds = df.as(Encoders.STRING());

        Dataset<String> wordDF = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }, Encoders.STRING());
        wordDF.show();


        Dataset<Tuple2<String, Object>> result = wordDF.groupByKey(new MapFunction<String, String>() {
            @Override
            public String call(String value) throws Exception {
                System.out.println("value = " + value);
                return value;
            }
        }, Encoders.STRING()).count();

        result.show();
    }

    /**
     * Typed Operation을 이용한 단어수 세기.
     *
     * @param spark
     * @param df
     */
    public static void runTypedTransformationsExample2(SparkSession spark, Dataset<Row> df) {
        // df는 Row 타입이기 때문에 원래의 타입인 String으로 변환하여 Dataset 생성.
        Dataset<String> ds = df.as(Encoders.STRING());

        // flatmap으로 단어 분리.
        Dataset<String> wordDF = ds.flatMap((String s) -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING());

        wordDF.show();

        // groupByKey key를 기준으로 groupby
        Dataset<Tuple2<String, Object>> result =
                wordDF.groupByKey((MapFunction<String, String>) (String s) -> s, Encoders.STRING()).count();


        //result.show();

        // dataset 저장.
        result.write().json("./dataset-output");
    }
}
