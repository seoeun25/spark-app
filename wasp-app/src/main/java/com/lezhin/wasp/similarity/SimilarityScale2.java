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
import scala.Tuple2;

import java.io.Serializable;
import java.time.Instant;
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
public class SimilarityScale2 {

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

    public static Dataset<Row> scoreCal(SparkSession spark, Dataset<Row> infoDf, Dataset<Row> scoreDf) {

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

        Row[] list = (Row[]) scoreDf.collect();
        System.out.println(" scoreDf list.size = " + list.length);

        for (Map.Entry<String, Float> entry : simDic.entrySet()) {
            String key = entry.getKey();
            int index = key.indexOf("_");
            Long sourceId = Long.valueOf(key.substring(0, index));
            Long targetId = Long.valueOf(key.substring(index + 1, key.length()));
            Float value = entry.getValue();

            float abScore = value
                    + Optional.ofNullable(simDic.get(targetId + "_" + sourceId)).orElse(0F);
            float abScoreSum = Optional.ofNullable(infoDic.get(sourceId)).orElse(0L)
                    + Optional.ofNullable(infoDic.get(targetId)).orElse(0L);
            float score = abScore / abScoreSum;
            scaleTable.put(key, Intersection.builder().key(key).sourceContentId(sourceId).targetContentId(targetId)
                    .scoreSum(score).build());
        }


        Dataset<Row> scaleDf = spark.createDataFrame(
                new ArrayList<>(scaleTable.values()), Intersection.class);
        scaleDf.printSchema();

        return scaleDf;
    }

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
        JavaRDD<SimilarityScale.Score> rdd = idxScaled.select(col("source_content_id")).distinct().toJavaRDD()
                .map(row -> SimilarityScale.Score.builder().key(String.valueOf(row.getLong(0) + "_" + row.getLong(0)))
                        .sourceContentId(row.getLong(0))
                        .targetContentId(row.getLong(0))
                        .score(0.0).build());
        Dataset<Row> selfDf = spark.createDataFrame(rdd, SimilarityScale.Score.class)
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

    public static void saveScale(SparkSession spark, Dataset<Row> scaledDf, String locale, String adult) {

        String tableName = String.format("actdb.content_similarity_%s%s", Utils.getLanguage(locale), adult);
        System.out.println("---- start save " + tableName);
        scaledDf.createOrReplaceTempView("content_similarity_tmp");
        spark.sql("drop table if exists " + tableName);
        spark.sql("create table " + tableName + " as select * from content_similarity_tmp");

        System.out.println("---- end save " + tableName);

    }

    public static void save(SparkSession spark, Dataset<Row> scaledDf, String locale, String adult) {

        String tableName = String.format("actdb.content_score_cal_%s%s", Utils.getLanguage(locale), adult);
        System.out.println("---- start save " + tableName);
        scaledDf.createOrReplaceTempView("score_cal_tmp");
        spark.sql("drop table if exists " + tableName);
        spark.sql("create table " + tableName + " as select key, scoresum, " +
                "sourceContentId as source_content_id, targetContentId as target_content_id from score_cal_tmp");

        System.out.println("---- end save " + tableName);

    }

    public static void main(String... args) {

        if (ArrayUtils.getLength(args) < 4) {
            System.out.println("Usage: SimilarityScoreCal <master> <ymd> <locale> <hive-metastore>");
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
        boolean test = false;
        if (args.length >= 6) {
            test = Boolean.valueOf("test".equals(args[5]));
        }
        System.out.println(String.format("master = %s, ymd = %s, locale = %s, adult=%s, metastore = %s",
                master, ymd, locale, adult, hiveMetastore));

        JavaSparkContext sc = getSparkContext("SimilarityScoreCal", master);

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
            // adult == 0 이면 전연령대상.
            String adultCondition = null;
            if (!"0".equals(adult)) {
                adultCondition = "and adult = " + adult;
            }
            String queryStr1 = String.format("SELECT user_id, content_id, purchase_cnt FROM actdb" +
                    ".purchase_count_similarity WHERE locale='%s' ", locale);
            if (adultCondition != null) {
                queryStr1 = queryStr1 + adultCondition;
            }

            if (test) {
                queryStr1 = queryStr1 + " and content_id <= 20 ";
            }
            System.out.println(" -- query : " + queryStr1);


            Dataset<Row> dfLoad = spark.sql(queryStr1).where(col("purchase_cnt").isNotNull());
            System.out.println("-- purchase_count_similarity .count = " + dfLoad.count());

            Dataset infoDf = infoDic(dfLoad);

            String tableName = String.format("actdb.content_score_%s%s", Utils.getLanguage(locale), adult);
            String queryStr2 = String.format("SELECT key, scoresum FROM " + tableName );

            System.out.println(" -- query : " + queryStr2);
            Dataset<Row> scoreDf = spark.sql(queryStr2).where(col("scoresum").isNotNull());
            System.out.println("-- content_score.count = " + scoreDf.count());
            scoreDf.show();

            System.out.println("----- start score_cal");
            Dataset<Row> df = scoreCal(spark, infoDf, scoreDf);
            //save(spark, df, locale, adult);

            Dataset<Row> dfScale = scale2(spark, scoreDf, locale, ymd, adult);
            //saveScale(spark, );


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
