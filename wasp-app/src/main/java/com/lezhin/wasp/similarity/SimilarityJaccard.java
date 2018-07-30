package com.lezhin.wasp.similarity;

import com.lezhin.wasp.util.Utils;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

/**
 * * To execute,
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimilarityJaccard target/wasp-app-0.9-SNAPSHOT.jar
 * local[*]
 * /usr/lib/spark/README.md
 * java-out
 * </p>
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimilarityJaccard --master yarn --deploy-mode client \
 * deploy/wasp-app.jar yarn abc ko-KR thrift://azra:9083
 * </p>
 *
 * @author seoeun
 * @since 2018.07.17
 */
public class SimilarityJaccard {

    @Builder
    @Getter
    @EqualsAndHashCode
    public static class Score implements Serializable {
        private String key;
        private long sourceContentId;
        private long targetContentId;
        private double score;
    }

    @Builder
    @Getter
    @EqualsAndHashCode
    public static class Intersection implements Serializable {
        private String key;
        private long sourceContentId;
        private long targetContentId;
        private float scoreSum;
    }

    @Builder
    @Getter
    @EqualsAndHashCode
    public static class Key implements Serializable {
        private String key;
        private long sourceContentId;
        private long targetContentId;
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

    public static Dataset<Row> jaccard(SparkSession spark, Dataset<Row> infoDf, Dataset<Row> scoreDf) {

        System.out.println("start jaccard");
        scoreDf.printSchema();

        infoDf.createOrReplaceTempView("infodic_tmp");
        infoDf.printSchema();

        JavaRDD<Intersection> rdd = scoreDf.toJavaRDD().map((Function<Row, Intersection>) row -> {
            String key = row.getString(0);
            int index = key.indexOf("_");
            Long sourceId = Long.valueOf(key.substring(0, index));
            Long targetId = Long.valueOf(key.substring(index + 1, key.length()));

            return new Intersection(key, sourceId, targetId, row.getFloat(1));
        });

        Dataset<Row> scoreDf1 = spark.createDataFrame(rdd, Intersection.class);
        scoreDf1.orderBy("sourceContentId", "targetContentId").show();

        Dataset<Row> scoreDf2 = scoreDf1.select(col("key"), col("sourceContentId"), col("targetContentId"),
                col("scoreSum"));
        //scoreDf1.show();

        scoreDf1.createOrReplaceTempView("score1_tmp");
        scoreDf2.createOrReplaceTempView("score2_tmp");

        String sql = "select a.key, a.sourceContentId, a.targetContentId, a.scoreSum as abscore, " +
                "b.scoresum as bascore, c.score as ascore, d.score as bscore " +
                "from score1_tmp a " +
                "left outer join score2_tmp b on ( a.sourceContentId = b.targetContentId " +
                "and a.targetContentId = b.sourceContentId ) " +
                "left outer join infodic_tmp c on (a.sourceContentId = c.content_id) " +
                "left outer join infodic_tmp d on (a.targetContentId = d.content_id) ";

        Dataset<Row> caldf = spark.sql(sql);
        System.out.println("start cal ---");
        caldf.printSchema();
        caldf.orderBy(col("sourceContentId"), col("targetContentId")).show();
        System.out.println("---- start jaccardDf");
        Dataset<Row> jaccardDf = spark.createDataFrame(
                caldf.toJavaRDD().map(row -> {
                    float abScore = row.getFloat(3) + row.getFloat(4);
                    float abScoreSum = row.getLong(5) + row.getLong(6);
                    float score = abScore / abScoreSum;
                    return Intersection.builder().key(row.getString(0)).sourceContentId(row.getLong(1))
                            .targetContentId(row.getLong(2)).scoreSum(score).build();

                }), Intersection.class);


        return jaccardDf;
    }

    public static Dataset<Row> scale(SparkSession spark, Dataset<Row> scoreDf, String locale, String ymd,
                                     String adultKind) {

        scoreDf.printSchema();
        scoreDf.createOrReplaceTempView("score_cal_tmp");

        System.out.println("scoreDf. size = " + scoreDf.count());
        //scaleDf.orderBy("sourceContentId", "target_content_id").show(100);

        System.out.println("--- start create col_ max_tmp");
        scoreDf.groupBy(col("targetcontentid").as("id")).agg(max("scoreSum").as("max"))
                .createOrReplaceTempView("max_tmp");  //max 는 source 에서 구하고 scale은 target 대상으로??
        System.out.println("--- end create col_ max_tmp");

        //spark.sql("select id, max from max_tmp order by id").show();


        //System.out.println("colMax. size = " + spark.sql("select count(target_content_id) from col_max_tmp"));

        System.out.println("--- start create col_scaled");
        String sql = "select a.key, a.sourceContentId, a.targetContentId, a.scoreSum, b.max from score_cal_tmp a " +
                "join max_tmp b on (a.targetContentId = b.id)";
        Dataset<Row> colScaled = spark.sql(sql).withColumn("scaled", col("scoreSum").divide(col("max")));
        colScaled.createOrReplaceTempView("scaled_tmp");

        //System.out.println("---- colScaled = " + colScaled.count());
        colScaled.printSchema();
        //colScaled.orderBy(col("sourceContentId"), col("target_content_id")).show();

        // idx scale
        System.out.println("--- start idx_max_tmp");
        colScaled.groupBy(col("sourcecontentid").as("id")).agg(max("scaled").as("max"))
                .createOrReplaceTempView("max_tmp");

        System.out.println("--- end idx_max_tmp");
        sql = "select a.key, a.sourcecontentid, a.targetcontentid, a.scaled, b.max from scaled_tmp a " +
                "join max_tmp b on (a.sourcecontentid = b.id)";
        Dataset<Row> idxScaled = spark.sql(sql)
                .withColumn("abscore", col("scaled").divide(col("max")))
                .select(col("key"), col("sourcecontentid"), col("targetcontentid"), col("abscore"));

        idxScaled.printSchema();
        System.out.println("---- idxScaled finish");
        idxScaled.show(10);

        // add self score
        JavaRDD<Score> rdd = idxScaled.select(col("sourcecontentid")).distinct().toJavaRDD()
                .map(row -> Score.builder().key(String.valueOf(row.getLong(0) + "_" + row.getLong(0)))
                        .sourceContentId(row.getLong(0))
                        .targetContentId(row.getLong(0))
                        .score(0.0).build());
        Dataset<Row> selfDf = spark.createDataFrame(rdd, Score.class)
                .select(col("key"), col("sourcecontentid"),
                        col("targetcontentid"),
                        col("score").as("abscore"));
        selfDf.na().fill(0);
        System.out.println("---- selfDf ");
        selfDf.printSchema();
        selfDf.show();


        long timestamp = Instant.now().toEpochMilli();
        Dataset<Row> union = idxScaled.unionAll(selfDf)
                .withColumn("locale", functions.lit(locale))
                .withColumn("ymd", functions.lit(ymd))
                .withColumn("adult_kind", functions.lit(adultKind))
                .withColumn("created_at", functions.lit(timestamp))
                .orderBy(col("sourceContentId"), col("targetContentId"));

        return union;
    }

    public static void saveScale(SparkSession spark, Dataset<Row> scaledDf, String locale, String adultKind) {

        scaledDf.printSchema();

        String tableName = String.format("actdb.content_similarity_%s%s", Utils.getLanguage(locale), adultKind);
        System.out.println("---- start save " + tableName);
        scaledDf.createOrReplaceTempView("content_similarity_tmp");
        spark.sql("drop table if exists " + tableName);
        spark.sql("create table " + tableName + " as select " +
                "key, sourceContentId as source_content_id, targetContentId as target_content_id, " +
                "abscore, locale, ymd, adult_kind, created_at " +
                "from content_similarity_tmp");

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
            System.out.println("Usage: SimilarityJaccard <master> <ymd> <locale> <adultKind> <hive-metastore>");
            return;
        }
        String master = args[0];
        String ymd = args[1];
        String locale = args[2];
        String adultKind = args[3];
        String hiveMetastore = "thrift://insight-v3-m:9083";
        if (args.length >= 5) {
            hiveMetastore = args[4];
        }
        boolean test = false;
        if (args.length >= 6) {
            test = Boolean.valueOf("test".equals(args[5]));
        }
        System.out.println(String.format("master = %s, ymd = %s, locale = %s, adultKind=%s, metastore = %s",
                master, ymd, locale, adultKind, hiveMetastore));

        JavaSparkContext sc = getSparkContext("SimilarityJaccard", master);

        try {

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
            // adultKind. 0 = 전연령 1 = 성인 2 = 완전판
            // adult == 0 이면 완전판 (성인물 + 비성인물). 대상이 모두 이므로 adult 조건은 필요 없음. (hasAdult)
            String adultCondition = null;
            if (!"2".equals(adultKind)) {
                adultCondition = "and adult = " + adultKind;
            }
            String queryStr1 = String.format("SELECT user_id, content_id, purchase_cnt FROM actdb" +
                    ".purchase_count_similarity WHERE locale='%s' ", locale);
            if (adultCondition != null) {
                queryStr1 = queryStr1 + adultCondition;
            }

            if (test) {
                queryStr1 = queryStr1 + " and content_id <= 4 ";
            }
            System.out.println(" -- query : " + queryStr1);


            Dataset<Row> dfLoad = spark.sql(queryStr1).where(col("purchase_cnt").isNotNull());
            System.out.println("-- purchase_count_similarity .count = " + dfLoad.count());

            Dataset infoDf = infoDic(dfLoad);

            String tableName = String.format("actdb.content_score_%s%s", Utils.getLanguage(locale), adultKind);
            String queryStr2 = String.format("SELECT key, scoresum FROM " + tableName);

            System.out.println(" -- query : " + queryStr2);
            Dataset<Row> scoreDf = spark.sql(queryStr2).where(col("scoresum").isNotNull());
            System.out.println("-- content_score.count = " + scoreDf.count());
            scoreDf.show();

            System.out.println("----- start score_cal");
            Dataset<Row> df = jaccard(spark, infoDf, scoreDf);
            //save(spark, df, locale, adult);

            Dataset<Row> dfScale = scale(spark, df, locale, ymd, adultKind);
            saveScale(spark, dfScale, locale, adultKind);


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
