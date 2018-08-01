package com.lezhin.wasp.similarity;

import com.lezhin.wasp.util.DatabaseProp;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * * To execute,
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimilarityScore target/wasp-app-0.9-SNAPSHOT.jar
 * local[*]
 * /usr/lib/spark/README.md
 * java-out
 * </p>
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.CurationExport --master yarn --deploy-mode client \
 * deploy/wasp-app.jar yarn production 20180729 0 thrift://azra:9083
 * </p>
 *
 * @author seoeun
 * @since 2018.07.17
 */
public class CurationExport {

    public static void exportToMysql(SparkSession spark, Properties conProps, Dataset<Row> exportDf, String tableName) {

        System.out.println(String.format("  Start export to %s, %s", conProps.get("url"), tableName));

        exportDf.write().jdbc(conProps.getProperty(""), tableName, conProps);

        // 이미 truncate 된 테이블에 append
        exportDf.write().mode(SaveMode.Append)
                .jdbc(conProps.getProperty("url"), tableName, conProps);

        System.out.println(String.format("  End export to %s, %s", conProps.get("url"), tableName));

    }

    public static void scheduleHistory(SparkSession spark, Properties conProps, String name, String result,
                                       String ymd) {

        System.out.println(String.format("Start scheduleHistory. %s", conProps.get("url")));

        ScheduleHistory.insertHistory(spark, name, result, ymd, conProps);

    }


    public static void main(String... args) throws Exception {
        System.out.println("args.length = " + args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.println("arg " + i + " : " + args[i]);
        }
        if (ArrayUtils.getLength(args) < 3) {
            System.out.println("Usage: CurationExport <master> <env> <ymd>");
            System.out.println("Usage: CurationExport yarn production 20180729");

            return;
        }

        String master = args[0];
        String env = args[1];
        String ymd = args[2];
        String hiveMetastore = "thrift://insight-v3-m:9083";
        if (args.length >= 4) {
            hiveMetastore = args[3];
        }

        System.out.println(String.format("master = %s, env = %s, ymd = %s, metastore = %s",
                master, env, ymd, hiveMetastore));

        JavaSparkContext sc = getSparkContext("CurationExport", master);

        try {

            String warehouseLocation = "spark-warehouse";
            SparkSession spark = SparkSession
                    .builder()
                    .appName("CurationExport")
                    //.config("spark.sql.warehouse.dir", warehouseLocation)
                    .config("hive.metastore.uris", hiveMetastore)
                    .enableHiveSupport()
                    .getOrCreate();

            Arrays.stream(sc.getConf().getAll()).forEach(tuple -> System.out.println("conf : " + tuple._1() + " = " +
                    tuple._2()));

            DatabaseProp databaseProp = DatabaseProp.valueOf(env.toUpperCase());
            if (databaseProp == null) {
                throw new Exception("Not supported database : " + env);
            }

            Properties serviceConn = new Properties();
            serviceConn.put("driver", "com.mysql.jdbc.Driver");
            serviceConn.put("url", databaseProp.getUrl());
            serviceConn.put("user", databaseProp.getUser());
            serviceConn.put("password", databaseProp.getPassword());
            serviceConn.put("autoReconnect", "true");
            serviceConn.put("useSSL", "false");
            serviceConn.put("useServerPrepStmts", "false");
            serviceConn.put("rewriteBatchedStatements", "true");

            int setNumber = DecisionSet.queryHistory(spark, serviceConn);
            System.out.println(" ---- setNumber = " + setNumber);

            // content_similarity_union
            String querySimilarity = String.format("SELECT locale, adult_kind, source_content_id, target_content_id, " +
                    "similarity, created_at, ymd FROM actdb.content_similarity_union ");

            System.out.println(" -- query(hive) : " + querySimilarity);
            Dataset<Row> similarityDf = spark.sql(querySimilarity);
            similarityDf.show(10);


            exportToMysql(spark, serviceConn, similarityDf, "content_similarity_" + setNumber);
            scheduleHistory(spark, serviceConn, "content_similarity", "content_similarity_" + setNumber, ymd);

            // content_similarity_set_union
            String querySet = String.format("SELECT locale, adult_kind, set_order, content_order, " +
                    "content_id, created_at, ymd FROM actdb" + ".content_similarity_set_union ");

            System.out.println(" -- query(hive) : " + querySet);
            Dataset<Row> setDf = spark.sql(querySet);
            setDf.show(10);
            //System.out.println("-- querySet.count = " + querySet.count());

            exportToMysql(spark, serviceConn, setDf, "content_similarity_set_" + setNumber);
            scheduleHistory(spark, serviceConn, "content_similarity_set", "content_similarity_set_" + setNumber, ymd);

            System.out.println("---- DONE !!!");

            spark.stop();

        } catch (Exception e) {
            System.out.println("---- error ----");
            e.printStackTrace();
            throw e;
        } finally {
            sc.stop();
        }

    }

    public static JavaSparkContext getSparkContext(String appName, String master) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        return new JavaSparkContext(conf);
    }


}