package com.lezhin.wasp.similarity;

import com.lezhin.wasp.util.Utils;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

/**
 * * To execute,
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimilarityScoreCalOld target/wasp-app-0.9-SNAPSHOT.jar
 * local[*]
 * /usr/lib/spark/README.md
 * java-out
 * </p>
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimilarityScoreCalOld --master yarn --deploy-mode client \
 * deploy/wasp-app.jar yarn abc ko-KR thrift://azra:9083
 * </p>
 *
 * @author seoeun
 * @since 2018.07.17
 */
public class SimilarityScaleOld {

    public static Dataset<Row> scale2(SparkSession spark, Dataset<Row> scoreDf, String locale, String ymd,
                                      String adult) {

        scoreDf.printSchema();
        scoreDf.createOrReplaceTempView("score_cal_tmp");

        System.out.println("scoreDf. size = " + scoreDf.count());
        //scaleDf.orderBy("sourceContentId", "target_content_id").show(100);

        System.out.println("--- start create col_ max_tmp");
        scoreDf.groupBy(col("target_Content_Id").as("id")).agg(max("scoreSum").as("max"))
                .createOrReplaceTempView("max_tmp");  //max 는 source 에서 구하고 scale은 target 대상으로??
        System.out.println("--- end create col_ max_tmp");

        //spark.sql("select id, max from max_tmp order by id").show();


        //System.out.println("colMax. size = " + spark.sql("select count(target_content_id) from col_max_tmp"));

        System.out.println("--- start create col_scaled");
        String sql = "select a.key, a.source_Content_Id, a.target_Content_Id, a.scoreSum, b.max from score_cal_tmp a " +
                "join max_tmp b on (a.target_Content_Id = b.id)";
        Dataset<Row> colScaled = spark.sql(sql).withColumn("scaled", col("scoreSum").divide(col("max")));
        colScaled.createOrReplaceTempView("scaled_tmp");

        //System.out.println("---- colScaled = " + colScaled.count());
        colScaled.printSchema();
        //colScaled.orderBy(col("sourceContentId"), col("target_content_id")).show();

        // idx scale
        System.out.println("--- start idx_max_tmp");
        colScaled.groupBy(col("source_content_id").as("id")).agg(max("scaled").as("max"))
                .createOrReplaceTempView("max_tmp");

        System.out.println("--- end idx_max_tmp");
        sql = "select a.key, a.source_content_id, a.target_content_id, a.scaled, b.max from scaled_tmp a " +
                "join max_tmp b on (a.source_content_id = b.id)";
        Dataset<Row> idxScaled = spark.sql(sql)
                .withColumn("abscore", col("scaled").divide(col("max")))
                .select(col("key"), col("source_content_id"), col("target_content_id"), col("abscore"));

        idxScaled.printSchema();
        System.out.println("---- idxScaled finish");
        idxScaled.show(10);

        // add self score
        JavaRDD<Score> rdd = idxScaled.select(col("source_content_id")).distinct().toJavaRDD()
                .map(row -> Score.builder().key(String.valueOf(row.getLong(0) + "_" + row.getLong(0)))
                        .sourceContentId(row.getLong(0))
                        .targetContentId(row.getLong(0))
                        .score(0.0).build());
        Dataset<Row> selfDf = spark.createDataFrame(rdd, Score.class)
                .select(col("key"), col("sourcecontentid").as("source_content_id"),
                        col("targetcontentid").as("target_content_id"),
                        col("score").as("abscore"));
        selfDf.na().fill(0);
        System.out.println("---- selfDf ");
        selfDf.printSchema();
        selfDf.show();


        long timestamp = Instant.now().toEpochMilli();
        Dataset<Row> union = idxScaled.unionAll(selfDf)
                .withColumn("locale", functions.lit(locale))
                .withColumn("ymd", functions.lit(ymd))
                .withColumn("adult", functions.lit(adult))
                .withColumn("created_at", functions.lit(timestamp))
                .orderBy(col("source_content_id"), col("target_content_id"));

        return union;
    }

    public static void save(SparkSession spark, Dataset<Row> scaledDf, String locale, String adult) {

        String tableName = String.format("actdb.content_similarity_%s%s", Utils.getLanguage(locale), adult);
        System.out.println("---- start save " + tableName);
        scaledDf.createOrReplaceTempView("content_similarity_tmp");
        spark.sql("drop table if exists " + tableName);
        spark.sql("create table " + tableName + " as select * from content_similarity_tmp");

        System.out.println("---- end save " + tableName);

    }

    public static void main(String... args) {

        if (ArrayUtils.getLength(args) < 4) {
            System.out.println("Usage: SimilarityScoreCalOld <master> <ymd> <locale> <hive-metastore>");
            return;
        }
        String master = args[0];
        String ymd = args[1];
        String locale = args[2];
        String adult = args[3];
        String hiveMetastore = "thrift://insight-v3-m:9083";
        if (args.length >= 5) {
            hiveMetastore = args[4];
        }
        System.out.println(String.format("master = %s, ymd = %s, locale = %s, adult = %s, metastore = %s",
                master, ymd, locale, adult, hiveMetastore));

        JavaSparkContext sc = getSparkContext("SimilarityScoreCalOld", master);

        try {

            //interval = spark.sparkContext.getConf().getAll()
            //print("-- interval = {}".format(interval))

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

            String tableName = String.format("actdb.content_score_cal_%s%s", Utils.getLanguage(locale), adult);
            String queryStr2 = String.format("SELECT * FROM " + tableName);

            System.out.println(" -- query : " + queryStr2);
            Dataset<Row> scoreDf = spark.sql(queryStr2).where(col("scoresum").isNotNull());
            System.out.println("-- content_score.count = " + scoreDf.count());


            scoreDf.show();

            System.out.println("----- start scale");
            Dataset<Row> df = scale2(spark, scoreDf, locale, ymd, adult);
            save(spark, df, locale, adult);

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

    @Builder
    @Getter
    @EqualsAndHashCode
    public static class Score implements Serializable {
        private String key;
        private long sourceContentId;
        private long targetContentId;
        private double score;
    }

}
