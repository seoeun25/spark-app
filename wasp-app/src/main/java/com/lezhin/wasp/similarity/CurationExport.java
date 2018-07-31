package com.lezhin.wasp.similarity;

import com.lezhin.wasp.model.ScheduleEvent;
import com.lezhin.wasp.util.DatabaseProp;

import com.google.common.collect.ImmutableList;
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

    public static int queryHistory(SparkSession spark, Properties conProps) {

        int setNumber;

        long lastTime = Instant.now().toEpochMilli() - (1000 * 60 * 60 * 24) * 5;
        String query = "(select name, result, ymd, created_at from schedule_event where created_at >= " + lastTime +
                ") history_alias";

        System.out.println(String.format("  Start getLatest set, %s, %s", conProps.get("url"), query));

        Dataset<Row> historyDf = spark.read().jdbc(conProps.get("url").toString(),
                query,
                conProps);

        List<Row> history = historyDf.orderBy(functions.desc("created_at")).collectAsList();
        if (history.size() == 0) {
            setNumber = 0;
        } else {
            Row lastestHistory = history.get(0);
            String result = lastestHistory.getString(1);
            System.out.println("-- latestHistory " + lastestHistory.toString());
            String set = result.substring(result.lastIndexOf("_") + 1);
            setNumber = Integer.parseInt(set);
        }

        System.out.println(String.format("  End getLatest set, %s, setNumber=%s", conProps.get("url"), setNumber));
        return setNumber;

    }

    public static void exportToMysql(SparkSession spark, Properties conProps, Dataset<Row> exportDf, String tableName) {

        System.out.println(String.format("  Start export to %s, %s", conProps.get("url"), tableName));

        exportDf.write().mode(SaveMode.Overwrite).option("truncate", true)
                .jdbc(conProps.getProperty("url"), tableName, conProps);

        System.out.println(String.format("  End export to %s, %s", conProps.get("url"), tableName));

    }

    public static void scheduleHistory(SparkSession spark, Properties conProps, String name, String result,
                                       String ymd) {

        System.out.println(String.format("Start scheduleHistory. %s", conProps.get("url")));

        long timestamp = Instant.now().toEpochMilli();
        List<ScheduleEvent> list = ImmutableList.of(ScheduleEvent.builder()
                .name(name)
                .result(result)
                .ymd(Integer.parseInt(ymd))
                .created_at(timestamp).build());
        System.out.println(" --  get(0) = " + list.get(0).toString());

        Dataset<Row> eventDf = spark.createDataFrame(list, ScheduleEvent.class);
        eventDf.write().mode("append").jdbc(conProps.getProperty("url"), "schedule_event", conProps);

        System.out.println(String.format("scheduleHistory. %s, name=%s, result=%s, ymd=%s, created_at=%s",
                conProps.get("url"), name, result, ymd, timestamp));

    }


    public static void main(String... args) {
        System.out.println("args.length = " + args.length);
        if (ArrayUtils.getLength(args) < 4) {
            System.out.println("Usage: CurationExport <master> <env> <ymd> <setNumber>");
            System.out.println("Usage: CurationExport yarn production 20180729 0");

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

            int setNumber = queryHistory(spark, serviceConn);
            System.out.println(" ---- setNumber = " + setNumber);

            String querySimilarity = String.format("SELECT locale, adult_kind, source_content_id, target_content_id, " +
                    "similarity, created_at, ymd FROM actdb" + ".content_similarity_union ");

            System.out.println(" -- query(hive) : " + querySimilarity);
            Dataset<Row> similarityDf = spark.sql(querySimilarity);
            similarityDf.show(10);


            exportToMysql(spark, serviceConn, similarityDf, "content_similarity_" + setNumber);
            scheduleHistory(spark, serviceConn, "content_similarity", "content_similarity_" + setNumber, ymd);

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
