package com.lezhin.wasp.similarity;

import com.lezhin.wasp.util.Utils;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

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

    /**
     */
    public static Dataset<Row> infoDic(Dataset<Row> df) {


        Dataset<Row> df2 = df.select(
                df.col("user_id"),
                df.col("content_id"),
                when(df.col("purchase_cnt").geq(10), 10)
                        .otherwise(df.col("purchase_cnt")).as("score"));

        System.out.println(" --- df2. count = " + df2.count());
        df2.show();
        Dataset<Row> dfInfoDic = df2.groupBy("content_id").sum("score");
        System.out.println(" --- dfInfoDic.count = " + dfInfoDic.count());
        dfInfoDic.show();


        return dfInfoDic;
    }


    public static Dataset<Row> userDic2(Dataset<Row> df) {

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

    public static Dataset<Row> simTable2(SparkSession spark, Dataset<Row> userDf) {

        userDf.printSchema();

        System.out.println("start simTable2 ");

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

    public static void save(SparkSession spark, Dataset<Row> scoreDf) {

        System.out.println("---- start save content_score");

        scoreDf.createOrReplaceTempView("content_score_tmp");
        spark.sql("drop table if exists actdb.content_score");
        spark.sql("create table actdb.content_score as select * from content_score_tmp");

        System.out.println("---- save content_score");

    }

    public static void main(String... args) {

        if (ArrayUtils.getLength(args) != 5) {
            System.out.println("Usage: SimilarityCluster <master> <ymd> <locale>");
            return;
        }
        String master = args[0];
        String ymd = args[1];
        String locale = args[2];
        String adult = args[3];
        String hiveMetastore = args[4];
        System.out.println(String.format("master = %s, ymd = %s, locale = %s, adult = %s, metastore = %s",
                master, ymd, locale, adult, hiveMetastore));

        JavaSparkContext sc = getSparkContext("SimilarityCluster", master);

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

            // adult == 1 이면 audult 조건은 필요 없음. 모두.
            String adultCondition = null;
            if ("1".equals(adult)) {
                adultCondition = "and adult = " + adult;
            }
            String queryStr = String.format("SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity " +
                    "WHERE locale='%s' ", locale);
            if (adultCondition !=null) {
                queryStr = queryStr + adultCondition;
            }

//            String queryStr = "SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity WHERE" +
//                    " locale='ko-KR' and  content_id <= 20 ";

            System.out.println(" -- query : " + queryStr);
            Dataset<Row> dfLoad = spark.sql(queryStr);
            System.out.println("-- dfLoad.count = " + dfLoad.count());


            Dataset<Row> cleanDf = dfLoad.where(dfLoad.col("purchase_cnt").isNotNull());
            cleanDf.show();
            System.out.println("-- cleanDf.count = " + dfLoad.count());

            Dataset infoDf = infoDic(cleanDf);
            Dataset userDf = userDic2(cleanDf);

            Dataset scoreDf = simTable2(spark, userDf);

            System.out.println("----- finish tableDic");

            save(spark, scoreDf);

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

    /**
     * Load from local filesystem.
     *
     * @param sc
     * @param input
     * @return
     */
    public static JavaRDD<String> getInputRdd(JavaSparkContext sc, String input) {
        return sc.textFile(input);
    }

    /**
     * Save to local filesystem.
     *
     * @param resultRdd
     * @param output
     */
    public static void saveResult(JavaPairRDD<String, Integer> resultRdd, String output) {
        System.out.println("---- output = " + output);
        resultRdd.saveAsTextFile(output);
    }

    public static JavaPairRDD<String, Integer> process(JavaRDD<String> inputRdd) {
        JavaRDD<String> words = inputRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> wcPair = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> result = wcPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        return result;
    }

    public static JavaPairRDD<String, Integer> processWithLambda(JavaRDD<String> inputRdd) {
        JavaRDD<String> words = inputRdd.flatMap((String s) -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> wcPair = words.mapToPair((String a) -> new Tuple2(a, 1));

        JavaPairRDD<String, Integer> result = wcPair.reduceByKey((Integer c1, Integer c2) -> c1 + c2);

        return result;

    }
    // spark-submit --class com.seoeun.ch1.scala.WordCount1 deploy/wikibook.jar local[*] /usr/lib/spark/README.md ~/spark-out/a

}
