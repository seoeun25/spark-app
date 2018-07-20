package com.lezhin.wasp.similarity;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

/**
 * * To execute,
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimilarityScoreCal target/wasp-app-0.9-SNAPSHOT.jar
 * local[*]
 * /usr/lib/spark/README.md
 * java-out
 * </p>
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimilarityScoreCal --master yarn --deploy-mode client \
 * deploy/wasp-app.jar yarn abc ko-KR thrift://azra:9083
 * </p>
 *
 * @author seoeun
 * @since 2018.07.17
 */
public class SimilarityScoreCal {

    @Builder
    @Getter
    @EqualsAndHashCode
    public static class Intersection implements Serializable {
        private String key;
        private long sourceContentId;
        private long targetContentId;
        private float scoreSum;
    }

    /**
     */
    public static Dataset<Row> infoDic(Dataset<Row> df) {

        Dataset<Row> df2 = df.select(
                df.col("user_id"),
                df.col("content_id"),
                when(df.col("purchase_cnt").geq(10), 10)
                        .otherwise(df.col("purchase_cnt")).as("score"));

        Dataset<Row> dfInfoDic = df2.groupBy("content_id").agg(sum("score").as("score"));
        System.out.println(" --- dfInfoDic.count = " + dfInfoDic.count());
        dfInfoDic.show();

        return dfInfoDic;
    }

    public static Dataset<Row> scale3(SparkSession spark, Dataset<Row> infoDf, Dataset<Row> scoreDf) {

        scoreDf.printSchema();

        int itemCount = new Integer(String.valueOf(infoDf.count()));
        System.out.println("infoDf. size = " + itemCount);

        Map<Long, Long> infoDic = infoDf.toJavaRDD()
                .mapToPair(row -> new Tuple2<>(row.getLong(0), row.getLong(1))).collectAsMap();
        System.out.println("infoDic. size = " + infoDic.size());

        Map<String, Float> simDic = scoreDf.toJavaRDD()
                .mapToPair(row -> new Tuple2<>(row.getString(0), row.getFloat(1))).collectAsMap();
        System.out.println("simDic . size = " + simDic.size());

        Map<String, Intersection> scaleTable = new HashMap<>();

        Row[] list = (Row[])scoreDf.collect();
        System.out.println(" scoreDf list.size = " + list.length);

        for (Map.Entry<String, Float> entry: simDic.entrySet()) {
            String key = entry.getKey();
            int index = key.indexOf("_");
            Long sourceId = Long.valueOf(key.substring(0, index));
            Long targetId = Long.valueOf(key.substring(index + 1, key.length()));
            Float value = entry.getValue();

            float abScore = value
                    + Optional.ofNullable(simDic.get(targetId+ "_" + sourceId)).orElse(0F);
            float abScoreSum = Optional.ofNullable(infoDic.get(sourceId)).orElse(0L)
                    + Optional.ofNullable(infoDic.get(targetId)).orElse(0L);
            float score = abScore / abScoreSum;
            scaleTable.put(key, Intersection.builder().key(key).sourceContentId(sourceId).targetContentId(targetId)
                    .scoreSum(score).build() );
        }


        Dataset<Row> scaleDf = spark.createDataFrame(
                new ArrayList<>(scaleTable.values()), Intersection.class);
        scaleDf.createOrReplaceTempView("score_cal_tmp");

        scaleDf.printSchema();

        return scaleDf;
    }

    public static void save(SparkSession spark, Dataset<Row> scaledDf) {

        System.out.println("---- start save content_score_cal");
        spark.sql("drop table if exists actdb.content_score_cal");
        spark.sql("create table actdb.content_score_cal as select key, scoresum, " +
                "sourceContentId as source_content_id, targetContentId as target_content_id from score_cal_tmp");
        //intersectionDF.orderBy(col("key")).show();

        System.out.println("---- end save content_score_cal");


    }

    public static void main(String... args) {

        if (ArrayUtils.getLength(args) != 4) {
            System.out.println("Usage: SimilarityScoreCal <master> <ymd> <locale> <hive-metastore>");
            return;
        }
        String master = args[0];
        String ymd = args[1];
        String locale = args[2];
        String hiveMetastore = args[3];
        System.out.println(String.format("master = %s, ymd = %s, locale = %s, metastore = %s", master, ymd, locale,
                hiveMetastore));

        JavaSparkContext sc = getSparkContext("SimilarityScoreCal", master);

        //String hiveMetastore = "thrift://azra:9083";
        //String hiveMetastore = "thrift://insight-v3-m:9083";


        try {

            //interval = spark.sparkContext.getConf().getAll()
            //print("-- interval = {}".format(interval))

            // $example on:spark_hive$
            // warehouseLocation points to the default location for managed databases and tables
            String warehouseLocation = "spark-warehouse";
            SparkSession spark = SparkSession
                    .builder()
                    .appName("Wasp-Similarity")
                    //.config("spark.sql.warehouse.dir", warehouseLocation)
                    .config("hive.metastore.uris", hiveMetastore)
                    .enableHiveSupport()
                    .getOrCreate();

            Arrays.stream(sc.getConf().getAll()).forEach(tuple -> System.out.println("conf : " + tuple._1() + " = " +
                    tuple._2()));
//            String queryStr1 = String.format("SELECT user_id, content_id, purchase_cnt " +
//                    "FROM actdb.purchase_count_similarity" +
//                    " WHERE locale='%s'", locale);

            String queryStr1 = "SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity WHERE" +
                    " locale='ko-KR' and  content_id <= 20 ";

            System.out.println(" -- query : " + queryStr1);
            Dataset<Row> dfLoad = spark.sql(queryStr1).where(col("purchase_cnt").isNotNull());
            System.out.println("-- purchase_count_similarity .count = " + dfLoad.count());

            Dataset infoDf = infoDic(dfLoad);

            String queryStr2 = String.format("SELECT key, scoresum FROM actdb.content_score ");

            //String queryStr2 = String.format("SELECT key, scoresum FROM actdb.content_score limit 100");

            System.out.println(" -- query : " + queryStr2);
            Dataset<Row> scoreDf = spark.sql(queryStr2).where(col("scoresum").isNotNull());
            System.out.println("-- content_score.count = " + scoreDf.count());
            scoreDf.show();

            System.out.println("----- start score_cal");
            Dataset<Row> df = scale3(spark, infoDf, scoreDf);
            save(spark, df);
            System.out.println("---- DONE !!!");


             /**


             spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
             spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");

             // Queries are expressed in HiveQL
             spark.sql("SELECT * FROM src").show();
             // +---+-------+
             // |key|  value|
             // +---+-------+
             // |238|val_238|
             // | 86| val_86|
             // |311|val_311|
             // ...

             // Aggregation queries are also supported.
             spark.sql("SELECT COUNT(*) FROM src").show();
             // +--------+
             // |count(1)|
             // +--------+
             // |    500 |
             // +--------+

             // The results of SQL queries are themselves DataFrames and support all normal functions.
             Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

             // The items in DaraFrames are of type Row, which lets you to access each column by ordinal.
             Dataset<String> stringsDS = sqlDF.map(new MapFunction<Row, String>() {
            @Override public String call(Row row) throws Exception {
            return "Key: " + row.get(0) + ", Value: " + row.get(1);
            }
            }, Encoders.STRING());
             stringsDS.show();
             // +--------------------+
             // |               value|
             // +--------------------+
             // |Key: 0, Value: val_0|
             // |Key: 0, Value: val_0|
             // |Key: 0, Value: val_0|
             // ...

             // You can also use DataFrames to create temporary views within a SparkSession.
             List<Record> records = new ArrayList<>();
             for (int key = 1; key < 100; key++) {
             Record record = new Record();
             record.setKey(key);
             record.setValue("val_" + key);
             records.add(record);
             }
             Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
             recordsDF.createOrReplaceTempView("records");

             // Queries can then join DataFrames data with data stored in Hive.
             spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
             // +---+------+---+------+
             // |key| value|key| value|
             // +---+------+---+------+
             // |  2| val_2|  2| val_2|
             // |  2| val_2|  2| val_2|
             // |  4| val_4|  4| val_4|
             // ...
             // $example off:spark_hive$

             */

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
