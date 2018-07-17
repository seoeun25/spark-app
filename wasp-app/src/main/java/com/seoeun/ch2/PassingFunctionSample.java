package com.seoeun.ch2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author seoeun
 * @since ${VERSION} on 6/6/17
 */
public class PassingFunctionSample {

    public void runMapSample(JavaSparkContext sc) {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> rdd2 = rdd1.map((Integer i) -> i + 1);
        //JavaRDD<Integer> rdd2 = rdd1.map(new Add());
    }

    class Add implements Function<Integer, Integer> {

        @Override
        public Integer call(Integer v1) throws Exception {
            return v1 + 1;
        }
    }
}
