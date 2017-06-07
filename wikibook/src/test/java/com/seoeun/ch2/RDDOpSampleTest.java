package com.seoeun.ch2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author seoeun
 * @since ${VERSION} on 6/7/17
 */
public class RDDOpSampleTest {

    private static JavaSparkContext sc;
    @BeforeClass
    public static void setupClass() {
        SparkConf conf = new SparkConf().setAppName("RDDOpSampleTest").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
    }

    @Test
    public void testCount() {

        RDDOpSample.doCount(sc);
    }

    @Test
    public void testMap() {
        RDDOpSample.doMap(sc);
        RDDOpSample.doFlatMap(sc);
    }

    @Test
    public void testMapPartition() {
        RDDOpSample.doMapPartition(sc);
        RDDOpSample.doMapPartitionsWithIndex(sc);
    }

    @Test
    public void testMapValues() {
        RDDOpSample.doMapValues(sc);
        RDDOpSample.doFlatMapValues(sc);
    }

    @Test
    public void testZip() {
        RDDOpSample.doZip(sc);
        RDDOpSample.doZipPartitions(sc);
    }

    @Test
    public void testGroupBy() {
        RDDOpSample.doGroupBy(sc);
        RDDOpSample.doGroupByKey(sc);
        RDDOpSample.doCoGroupBy(sc);
    }

    @Test
    public void testAggregations() {
        RDDOpSample.doDistinct(sc);
        RDDOpSample.doCartesian(sc);
        RDDOpSample.doSubtract(sc);
        RDDOpSample.doUnion(sc);
        RDDOpSample.doIntersection(sc);
    }

    @Test
    public void testJoin() {
        RDDOpSample.doJoin(sc);
        RDDOpSample.doOuterJoin(sc);
    }

    @Test
    public void testSubtractByKey() {
        RDDOpSample.doSubtractByKey(sc);
    }

    @Test
    public void testAggregation() {
        RDDOpSample.doReduceByKey(sc);
        RDDOpSample.doFoldByKey(sc);
    }

    @Test
    public void testCombineByKey() {
        RDDOpSample.doCombineByKey(sc);
        RDDOpSample.doAggregateByKey(sc);
    }

    @Test
    public void testPipe() {
        RDDOpSample.doPipe(sc);
    }

    @Test
    public void testPartiton() {
        RDDOpSample.doPartition(sc);
        RDDOpSample.doRepartitionAndSortWithinPartitions(sc);
        RDDOpSample.doPartitionBy(sc);
    }

    @Test
    public void testFilter() {
        RDDOpSample.doFilter(sc);
    }

    @Test
    public void testSortByKey() {
        RDDOpSample.doSortByKey(sc);
    }

    @Test
    public void testKeysValues() {
        RDDOpSample.doKeysValues(sc);
    }

    @Test
    public void testFirstTakeCount() {
        RDDOpSample.doFrist(sc);
    }

    @Test
    public void testActions() {
        RDDOpSample.doCountByValue(sc);
    }

    @Test
    public void testReduce() {
        RDDOpSample.doReduce(sc);
    }

    @Test
    public void testFold() {
        RDDOpSample.doFold(sc);
    }

    @Test
    public void testAggregate() {
        RDDOpSample.doAggregate(sc);
    }

    @Test
    public void testSum() {
        RDDOpSample.doSum(sc);
    }

    @Test
    public void testForEach() {
        RDDOpSample.doForEach(sc);
    }

    @Test
    public void testToDebugString() {
        RDDOpSample.doDebugString(sc);
    }

    @Test
    public void testCache() {
        RDDOpSample.doCache(sc);
    }

    @Test
    public void testPartitions() {
        RDDOpSample.doPartitions(sc);
    }

    @Test
    public void testSaveAsText() {
        RDDOpSample.saveAndLoadTextFile(sc);
    }

    @Test
    public void testSaveAsObjectFile() {
        RDDOpSample.saveAndLoadObjectFile(sc);
    }

    @Test
    public void testSaveAsSequenceFile() {
        RDDOpSample.saveAndLoadSequenceFile(sc);
    }

    @Test
    public void testBroadcast() {
        RDDOpSample.doBroadCast(sc);
    }

    @Test
    public void testAccumulator() {
        RDDOpSample.doAccumulator(sc);
    }





}
