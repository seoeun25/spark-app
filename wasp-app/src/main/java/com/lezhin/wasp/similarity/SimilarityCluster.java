package com.lezhin.wasp.similarity;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.when;

/**
 * * To execute,
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimiarityCluster target/wasp-app-0.9-SNAPSHOT.jar
 * local[*]
 * /usr/lib/spark/README.md
 * java-out
 * </p>
 * <p>
 * $ spark-submit --class com.lezhin.wasp.similarity.SimiarityCluster --master yarn --deploy-mode client \
 * deploy/wasp-app.jar yarn hdfs://sembp:8020/sample/README.md hdfs://sembp:8020/sample/out
 * </p>
 *
 * @author seoeun
 * @since 2018.07.17
 */
public class SimilarityCluster {

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

    public static class Record2 implements Serializable {
        private long userId;
        private long contentId;
        private long purchaseCnt;

        public Record2 (long userId, long contentId, long purchaseCnt) {
            this.userId = userId;
            this.contentId = contentId;
            this.purchaseCnt = purchaseCnt;
        }

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public long getContentId() {
            return contentId;
        }

        public void setContentId(long contentId) {
            this.contentId = contentId;
        }

        public long getPurchaseCnt() {
            return purchaseCnt;
        }

        public void setPurchaseCnt(long purchaseCnt) {
            this.purchaseCnt = purchaseCnt;
        }
    }

    public static class Similiarity implements Serializable {
        private String key;
        private long sourceContentId;
        private long targetContentId;
        private double score;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public long getSourceContentId() {
            return sourceContentId;
        }

        public void setSourceContentId(long sourceContentId) {
            this.sourceContentId = sourceContentId;
        }

        public long getTargetContentId() {
            return targetContentId;
        }

        public void setTargetContentId(long targetContentId) {
            this.targetContentId = targetContentId;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }
    }

    /**
     */
    public static Dataset<Row> infoDic(Dataset<Row> df) {


        Dataset df2 = df.select(
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

    public static Dataset<Row> userDic(Dataset<Row> df) {


        Dataset dfUser = df.select(df.col("user_id"),
                map(col("content_id"),
                        when(df.col("purchase_cnt").geq(10), 10).otherwise(df.col("purchase_cnt")).as("score")
                ).as("cmap"));

        //System.out.println("dfUser.count = " + dfUser.count());
        //dfUser.show();

        Dataset dfUser2 = dfUser.groupBy(col("user_id")).agg(collect_list(col("cmap")).as("content_list"));
        System.out.println("dfUser2.count = " + dfUser2.count());
        dfUser2.show();

        return dfUser2;
    }

    public static void sim_table(Dataset<Row> infoDf, Dataset<Row> userDf) {

        userDf.printSchema();

        int itemCount = new Integer(String.valueOf(infoDf.count()));
        System.out.println("itemCount = " + itemCount);

        Map<String, Similiarity> table = new HashMap<>();

//        int[][] table = new int[itemCount][itemCount];
//        for (int i = 0; i < itemCount; i++) {
//            for (int j = 0; j < itemCount; j++) {
//                table[i][j] = 0;
//            }
//        }

        List<Row> list = userDf.toJavaRDD().collect();
        System.out.println(" userDf list.size = " + list.size());

        final int[] userCount = {0};
        System.out.println("list.size = " + list.size());
        for (int i = 0; i < list.size(); i++) {
            Row row = list.get(i);
            Long userId = row.getLong(0);
            List<Record2> purchaseRecord = new ArrayList<>();
            List contents = row.getList(1);
            //System.out.println("contents size = " + contents.size());
            if (contents.size() == 5) {
                System.out.println("user = " + row.get(0) + ", content_purchase =" + contents.size());
            }
            for (int a = 0; a < contents.size(); a++) {
                Map record = JavaConversions.mapAsJavaMap((scala.collection.immutable.Map) contents.get(a));
                //System.out.println("content. size = " + contents.size());
                if (record.size() != 1) {
                    System.out.println(" ===== record should be 1 map");
                    throw new RuntimeException("record should be 1 map");
                }
                Map.Entry<Long, Long> contentPurchase = (Map.Entry) record.entrySet().toArray()[0];
                //System.out.println("---- key = " + contentPurchase.getKey().getClass().getName() + " , value = " +
                //        contentPurchase.getValue().getClass().getName());
                Long contentId = contentPurchase.getKey();
                Long purchaseCount = contentPurchase.getValue();
                //System.out.println("---- contentId = " + contentId + " , count = " + purchaseCount);
                purchaseRecord.add(new Record2(userId, contentId, purchaseCount));
            }

            userCount[0]++;
        }

//        userDf.toJavaRDD().foreach(row -> {
//            int count = userCount[0];
//            System.out.println("--- count = " + count);
//            count +=1;
//            if (count < 100) {
//                System.out.println("row(0) = " + row.get(0));
//                System.out.println("row(1) = " + row.get(1).getClass().getTypeName());
//            }
//            userCount[0] = count;
//        });


        System.out.println(" --- userCountt[0] = " + userCount[0]);


        /**
         #비어있는 딕셔너리 생성
         for item in item_dic:
         result.setdefault(item, {})
         for other in item_dic:
         if item!=other:
         result[item][other]=0

         for user in user_dic:
         #중복없는 조합을 만들어서 계산량을 최소화 (ver1은 3시간 -> 현재 ver7은 10분)
         for tup in itertools.combinations(user_dic[user].keys(), 2):
         (item,other)=tup
         #중복없는 조합이기 때문에 한 번에 쌍으로 저장
         #구매건수 10건 이상은 10점으로 처리
         if user_dic[user][item] >= 10: result[item][other]+=10
         else: result[item][other]+=user_dic[user][item]
         if user_dic[user][other] >= 10: result[other][item]+=10
         else: result[other][item]+=user_dic[user][other]*/


    }


    public static void main(String... args) {

        if (ArrayUtils.getLength(args) != 4) {
            System.out.println("Usage: SimilarityCluster <master> <ymd> <locale>");
            return;
        }
        String master = args[0];
        String ymd = args[1];
        String locale = args[2];
        String hiveMetastore = args[3];
        System.out.println(String.format("master = %s, ymd = %s, locale = %s, metastore = %s", master, ymd, locale,
                hiveMetastore));

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

            String queryStr = String.format("SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity " +
                    "WHERE " +
                    "locale='%s'", locale);

//            String queryStr = "SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity WHERE" +
//                    " locale='ko-KR' and  content_id IN (1, 2, 3) ";

            System.out.println(" -- query : " + queryStr);
            Dataset<Row> dfLoad = spark.sql(queryStr);
            dfLoad.show();
            System.out.println("-- dfLoad.count = " + dfLoad.count());

            Dataset infoDf = infoDic(dfLoad);
            Dataset userDf = userDic(dfLoad);

            sim_table(infoDf, userDf);

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
