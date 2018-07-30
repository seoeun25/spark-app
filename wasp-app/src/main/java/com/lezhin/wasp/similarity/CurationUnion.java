package com.lezhin.wasp.similarity;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * * To execute,
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.CurationUnion target/wasp-app-0.9-SNAPSHOT.jar
 * local[*]
 * /usr/lib/spark/README.md
 * java-out
 * </p>
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.CurationUnion --master yarn --deploy-mode client \
 * deploy/wasp-app.jar yarn abc ko-KR thrift://azra:9083
 * </p>
 *
 * @author seoeun
 * @since 2018.07.17
 */
public class CurationUnion {

    public static void saveContentSimilarityUnion(SparkSession spark, Dataset<Row> unionDf) {

        String tableName = String.format("actdb.content_similarity_union");
        System.out.println("---- start save " + tableName);
        unionDf.createOrReplaceTempView("content_similarity_union_tmp");
        spark.sql("DROP TABLE IF EXISTS " + tableName);
        spark.sql("CREATE TABLE " + tableName + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS \n" +
                " SELECT * FROM content_similarity_union_tmp");

        System.out.println("---- end save " + tableName);

    }

    public static void saveContentSimilaritySetUnion(SparkSession spark, Dataset<Row> unionDf) {

        String tableName = String.format("actdb.content_similarity_set_union");
        System.out.println("---- start save " + tableName);
        unionDf.createOrReplaceTempView("content_similarity_set_union_tmp");
        spark.sql("DROP TABLE IF EXISTS " + tableName);
        spark.sql("CREATE TABLE " + tableName + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS \n" +
                " SELECT * FROM content_similarity_set_union_tmp");

        System.out.println("---- end save " + tableName);
    }

    public static void main(String... args) {

        if (ArrayUtils.getLength(args) < 2) {
            System.out.println("Usage: CurationUnion <master> <ymd> <hive-metastore>");
            return;
        }
        String master = args[0];
        String ymd = args[1];
        String hiveMetastore = "thrift://insight-v3-m:9083";
        if (args.length >= 3) {
            hiveMetastore = args[2];
        }
        boolean test = false;
        if (args.length >= 6) {
            test = Boolean.valueOf("test".equals(args[5]));
        }
        System.out.println(String.format("master = %s, ymd = %s, metastore = %s",
                master, ymd, hiveMetastore));

        JavaSparkContext sc = getSparkContext("Union", master);

        try {

            String warehouseLocation = "spark-warehouse";
            SparkSession spark = SparkSession
                    .builder()
                    .appName("Wasp-Curation")
                    //.config("spark.sql.warehouse.dir", warehouseLocation)
                    .config("hive.metastore.uris", hiveMetastore)
                    .enableHiveSupport()
                    .getOrCreate();

            Arrays.stream(sc.getConf().getAll()).forEach(tuple -> System.out.println("conf : " + tuple._1() + " = " +
                    tuple._2()));
            // adult == 0 이면 전연령대상. adult ==1 은 완전판(비성인물 + 성인물 = all)

            List<String> zoneNames = ImmutableList.of("ko0", "ko1", "ko2", "ja0", "ja2", "en0", "en2");
            String query = String.format("SELECT locale, adult_kind, source_content_id, target_content_id, abscore as" +
                    " " +
                    "similarity, created_at, ymd FROM %s_", "actdb.content_similarity");
            String queryStr1 = "";
            for (int i = 0; i < zoneNames.size(); i++) {
                queryStr1 += query + zoneNames.get(i);
                if (i != zoneNames.size() - 1) {
                    queryStr1 += " \n union \n";
                }
            }

            System.out.println(" -- query : \n" + queryStr1);

            Dataset<Row> unionDf = spark.sql(queryStr1);
            System.out.println("-- count_similarity_uion .count = " + unionDf.count());
            saveContentSimilarityUnion(spark, unionDf);

            String query2 = String.format("SELECT locale, adult_kind, set_id as set_order, content_order, content_id," +
                    " " +
                    "created_at, ymd FROM %s_", "actdb.content_similarity_set");
            String queryStr2 = "";
            for (int i = 0; i < zoneNames.size(); i++) {
                queryStr2 += query2 + zoneNames.get(i);
                if (i != zoneNames.size() - 1) {
                    queryStr2 += " \n union \n";
                }
            }
            Dataset<Row> unionSetDf = spark.sql(queryStr2);
            System.out.println("-- count_similarity_set_uion .count = " + unionSetDf.count());
            saveContentSimilaritySetUnion(spark, unionSetDf);

            System.out.println("---- DONE !!!");

            spark.stop();

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

}
