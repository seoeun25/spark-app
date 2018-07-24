package com.lezhin.wasp.similarity;

import com.lezhin.wasp.util.Utils;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.concat_ws;


/**
 * * To execute,
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimilarityScore target/wasp-app-0.9-SNAPSHOT.jar
 * local[*]
 * /usr/lib/spark/README.md
 * java-out
 * </p>
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimilarityScore --master yarn --deploy-mode client \
 * deploy/wasp-app.jar yarn abc ko-KR thrift://azra:9083
 * </p>
 *
 * @author seoeun
 * @since 2018.07.17
 */
public class SimilarityScore {

    @Builder
    @Getter
    @EqualsAndHashCode
    public static class Record2 implements Serializable {
        private long userId;
        private long contentId;
        private long purchaseCnt;
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

    public static Dataset<Row> userDf(Dataset<Row> df) {

        Dataset dfUser = df.select(df.col("user_id"),
                concat_ws("_", col("content_id"),
                when(df.col("purchase_cnt").geq(10), 10).otherwise(df.col("purchase_cnt")).as("score"))
                        .as("content_score")
        );

        System.out.println("dfUser.count = " + dfUser.count());
        dfUser.printSchema();
        dfUser.show();

        Dataset dfUser2 = dfUser.groupBy(col("user_id")).agg(collect_list(col("content_score")).as("content_list"));
        System.out.println("dfUser2.count = " + dfUser2.count());
        dfUser2.show();

        return dfUser2;
    }

    public static Dataset<Row> scoreDf(SparkSession spark, Dataset<Row> userDf) {

        userDf.printSchema();

        System.out.println("start scoreDf ");

        Map<String, Intersection> table = new HashMap<>();

        Row[] list = (Row[])userDf.collect();
        System.out.println(" userDf list.size = " + list.length);

        final int[] userCount = {0};
        System.out.println("list.size = " + list.length);
        for (int i = 0; i < list.length; i++) {
            Row row = list[i];
            Long userId = row.getLong(0);
            //System.out.println("userId = " + userId);
            Map<Long, Record2> purchaseRecord = new HashMap<>(); // contentId / Record2
            List<String> contents = row.getList(1);
            //System.out.println("contents size = " + contents.size());
            if (contents.size() == 5) {
                //System.out.println("user = " + row.get(0) + ", content_purchase =" + contents.size());
            }
            if (i % 5000 == 0) {
                System.out.println("processing : " + i );
            }
            for (int a = 0; a < contents.size(); a++) {
                String content_purchase = contents.get(a);
                //System.out.println("content_purchase = " + content_purchase);
                try {
                    int index = content_purchase.indexOf("_");
                    Long contentId = Long.valueOf(content_purchase.substring(0, index));
                    Long purchaseCount = Long.valueOf(content_purchase.substring(index + 1, content_purchase.length()));

                    purchaseRecord.put(contentId,
                            Record2.builder().contentId(contentId).userId(userId).purchaseCnt(purchaseCount).build());
                } catch (Exception e) {
                    System.out.println(String.format(
                            "!!! WARN. Failed to create record. user = %s, content_purchase = %s, msg = %s",
                            userId, content_purchase, e.getMessage()));
                    continue;
                }

            }
            // combination2 from contentList
            List<Long> contentIds = purchaseRecord.keySet().stream().collect(Collectors.toList());
            if (contentIds.size() > 1) {
                //System.out.println("user = " + userId + ", contentIds =  " + contentIds);
            }
            List<List<Long>> combinator = Utils.combinator(contentIds);
            for (List<Long> comb: combinator) {
                Long sourceContentId = comb.get(0);
                Long targetContentId = comb.get(1);
                String key = sourceContentId + "_" + targetContentId;
                Intersection newIntersection = Intersection.builder().key(key)
                        .sourceContentId(sourceContentId).targetContentId(targetContentId)
                        .scoreSum(purchaseRecord.get(sourceContentId).getPurchaseCnt()).build();

                Intersection intersection = table.get(key);
                if (intersection == null) {
                    table.put(key, newIntersection);
                } else {
                    Intersection merge = Intersection.builder().key(key)
                            .sourceContentId(sourceContentId).targetContentId(targetContentId)
                            .scoreSum(intersection.getScoreSum() + newIntersection.getScoreSum()).build();
                    table.put(key, merge);
                }
                //System.out.println(sourceContentId + " = " + intersection1.toString());
            }

            userCount[0]++;
        }

        System.out.println(" --- userCount[0] = " + userCount[0]);

        System.out.println("---- intersection. org. size = " + table.size());

        ArrayList<Intersection> list1 = new ArrayList<>(table.values());

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Broadcast<ArrayList<Intersection>> broadcastVar = jsc.broadcast(list1);

        Dataset<Row> df =spark.createDataFrame(
                broadcastVar.getValue(), Intersection.class).select(col("key"), col("scoreSum"));
        df.show();

        System.out.println("---- finish create score df, broadcast");
        return df;
    }

    public static void save(SparkSession spark, Dataset<Row> scoreDf, String locale, String adult) {

        String tableName = String.format("actdb.content_score_%s%s", Utils.getLanguage(locale), adult);
        System.out.println("---- start save " + tableName);
        scoreDf.createOrReplaceTempView("content_score_tmp");
        spark.sql("drop table if exists " + tableName);
        spark.sql("create table " + tableName + " as select * from content_score_tmp");
        System.out.println("---- end save " + tableName);

    }

    public static void main(String... args) {
        System.out.println("args.length = " + args.length);
        if (ArrayUtils.getLength(args) < 4) {
            System.out.println("Usage: SimilarityCluster <master> <ymd> <locale>");
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
        System.out.println(String.format("master = %s, ymd = %s, locale = %s, adult = %s, metastore = %s",
                master, ymd, locale, adult, hiveMetastore));

        JavaSparkContext sc = getSparkContext("SimilarityCluster", master);

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

            // adult == 0 이면 완전판 (성인물 + 비성인물). 대상이 모두 이므로 adult 조건은 필요 없음. (hasAdult)
            String adultCondition = null;
            if (!"1".equals(adult)) {
                adultCondition = "and adult = " + adult;
            }
            String queryStr = String.format("SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity " +
                    "WHERE locale='%s' ", locale);
            if (adultCondition !=null) {
                queryStr = queryStr + adultCondition;
            }

            if (test) {
                queryStr = queryStr + " and content_id <= 20 ";
            }

            System.out.println(" -- query : " + queryStr);
            Dataset<Row> dfLoad = spark.sql(queryStr);
            System.out.println("-- dfLoad.count = " + dfLoad.count());

            Dataset<Row> cleanDf = dfLoad.where(dfLoad.col("purchase_cnt").isNotNull());
            cleanDf.show();
            System.out.println("-- cleanDf.count = " + dfLoad.count());

            Dataset userDf = userDf(cleanDf);

            Dataset scoreDf = scoreDf(spark, userDf);

            System.out.println("----- finish scoreDf. tableDic");

            save(spark, scoreDf, locale, adult);

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
