package com.lezhin.wasp.similarity;

import com.lezhin.wasp.util.DatabaseProp;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
public class DecisionSet {

    public static int queryHistory(SparkSession spark, Properties conProps) {

        int setNumber;

        long lastTime = Instant.now().toEpochMilli() - (1000 * 60 * 60 * 24) * 5;
        String query = "(select name, result, ymd, created_at from schedule_event where created_at >= " + lastTime +
                ") history_alias";

        System.out.println(String.format("  Start getLatest set, %s, %s", conProps.get("url"), query));

        Dataset<Row> historyDf = spark.read().jdbc(conProps.get("url").toString(),
                query,
                conProps);
        System.out.println("---- historyDf.size = " + historyDf.count());

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
        setNumber = setNumber + 1;
        setNumber = setNumber % 2;

        System.out.println(String.format("  End getLatest set, %s, setNumber=%s", conProps.get("url"), setNumber));
        return setNumber;

    }

    public static String saveSetTable(SparkSession spark, int setNumber) {
        spark.sql("drop table if exists set0");
        spark.sql("drop table if exists set1");

        StructField f1 = DataTypes.createStructField("pname", DataTypes.StringType, true);
        StructField f2 = DataTypes.createStructField("setnumber", DataTypes.IntegerType, true);
        StructType schema1 = DataTypes.createStructType(Arrays.asList(f1, f2));

        Row r1 = RowFactory.create("tableset", setNumber);
        Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(r1), schema1);

        String tableName = "actdb.set" + setNumber;
        df1.createOrReplaceTempView("set_tmp");
        spark.sql("drop table if exists " + tableName);
        spark.sql("create table " + tableName + " as select * from set_tmp");
        System.out.println("---- end save " + tableName);
        return tableName;
    }

    public static void main(String... args){
        System.out.println("args.length = " + args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.println("arg " + i + " : " + args[i]);
        }
        if (ArrayUtils.getLength(args) < 3) {
            System.out.println("Usage: CurationExport <master> <env> <ymd>");
            System.out.println("Usage: CurationExport yarn production 20180729");
            System.exit(-1);
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

        JavaSparkContext sc = getSparkContext("DesionSet", master);

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

            String tableName = saveSetTable(spark, setNumber);

            System.out.println("---- DONE !!!");

            spark.stop();

        } catch (Exception e) {
            System.out.println("---- error ----");
            e.printStackTrace();
            System.exit(-2);
        } finally {
            sc.stop();
        }

    }

    public static JavaSparkContext getSparkContext(String appName, String master) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        return new JavaSparkContext(conf);
    }


}
