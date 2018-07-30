package com.lezhin.wasp.similarity;

import com.lezhin.wasp.model.ScheduleEvent;
import com.lezhin.wasp.util.DatabaseProp;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
 * $ spark-submit --class com.lezhin.wasp.similarity.ScheduleHistory --master yarn --deploy-mode client \
 * deploy/wasp-app.jar yarn qa content_similarity content_similarity_0 20180726 thrift://azra:9083
 * </p>
 *
 * @author seoeun
 * @since 2018.07.17
 */
public class ScheduleHistory {

    public static void main(String... args) {
        System.out.println("args.length = " + args.length);
        for (int i = 0;  i < args.length; i++) {
            System.out.println("arg " + i + " = " + args[i]);
        }
        if (ArrayUtils.getLength(args) < 5) {
            System.out.println("Usage: ScheduleHistory <master> <env> <name> <result> <ymd>");
            System.out.println("Usage: ScheduleHistory yarn production content_similarity " +
                    "content_similarity_0 20180729");

            return;
        }

        String master = args[0];
        String env = args[1];
        String name = args[2];
        String result = args[3];
        String ymd = args[4];
        String hiveMetastore = "thrift://insight-v3-m:9083";
        if (args.length >= 6) {
            hiveMetastore = args[5];
        }
        boolean test = false;
        if (args.length >= 6) {
            test = Boolean.valueOf("test".equals(args[5]));
        }
        System.out.println(String.format("master = %s, env = %s, name = %s, result = %s, ymd = %s, metastore = %s",
                master, env, name, result, ymd, hiveMetastore));

        JavaSparkContext sc = getSparkContext("ScheduleHistory", master);

        try {

            String warehouseLocation = "spark-warehouse";
            SparkSession spark = SparkSession
                    .builder()
                    .appName("ScheduleHistory")
                    .getOrCreate();

            DatabaseProp databaseProp = DatabaseProp.valueOf(env.toUpperCase());
            if (databaseProp == null) {
                throw new Exception("Not supported database : " + env);
            }

            Properties conProps = new Properties();
            conProps.put("driver", "com.mysql.jdbc.Driver");
            conProps.put("url", databaseProp.getUrl());
            conProps.put("user", databaseProp.getUser());
            conProps.put("password", databaseProp.getPassword());

            Arrays.stream(sc.getConf().getAll()).forEach(tuple -> System.out.println("conf : " + tuple._1() + " = " +
                    tuple._2()));

//            Dataset<Row> jdbcDf = spark.read().jdbc(conProps.get("url").toString(),
//                    "schedule_event",
//                    conProps);
//            System.out.println("jdbcDf . size = " + jdbcDf.count());
//            jdbcDf.printSchema();
//            jdbcDf.show();

            List<ScheduleEvent> list = ImmutableList.of(ScheduleEvent.builder()
                    .name(name)
                    .result(result)
                    .ymd(Integer.parseInt(ymd))
                    .created_at(Instant.now().toEpochMilli()).build());
            System.out.println(" --  get(0) = " + list.get(0).toString());

            Dataset<Row> eventDf = spark.createDataFrame(list, ScheduleEvent.class);
            eventDf.write().mode("append").jdbc(conProps.getProperty("url"), "schedule_event", conProps);

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
