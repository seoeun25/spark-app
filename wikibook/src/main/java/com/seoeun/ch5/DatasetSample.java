package com.seoeun.ch5;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.janino.Java;
import scala.Array;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * DataFrame은 Row 타입으로 구성된(Dataset[Row]) dataset을 말한다. Dataset[Int], Dataset[String] 등은
 * 그냥 dataset이라 부른다. Dataset[Row] 인 dataframe 은 사용 가능한 transformation 연산 종류가 달라진다.
 *
 * @author seoeun
 * @since ${VERSION} on 8/13/17
 */
public class DatasetSample {

    public static void main(String... args) {
        SparkSession spark = SparkSession.builder()
                .appName("DatasetSample")
                .master("local[*]")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();

        Person row1 = new Person("hayoon", 7, "student");
        Person row2 = new Person("sunwoo", 13, "student");
        Person row3 = new Person("hajoo", 5, "kindergartener");
        Person row4 = new Person("jinwoo", 13, "student");

        Product d1 = new Product("store2", "note", 20, 2000);
        Product d2 = new Product("store2", "bag", 10, 5000);
        Product d3 = new Product("store1", "note", 15, 1000);
        Product d4 = new Product("store1", "pen", 20, 5000);
        Dataset<Row> sampleDF2 = spark.createDataFrame(Arrays.asList(d1, d2, d3, d4), Product.class);


        // java collection을 이용한 리플렉션 방식의 dataset 생성.
        List<Person> data = Arrays.asList(row1, row2, row3, row4);
        Dataset<Row> df = spark.createDataFrame(data, Person.class);

//        ds.printSchema();
//        System.out.println("-----------");
//        ds.show();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        //createDataset(spark, sc);
        //runBasicOperations(spark, sc, ds);
        //runColumnEx(spark, sc, ds);
        //runAliasEx(spark, sc, ds);
        //runIsIn(spark, sc, ds);
        //runWhen(spark, sc, ds);
        //runMaxMin(spark, ds);
        runAggregationFunctions(spark, df, sampleDF2);

    }

    public static void createDataset(SparkSession spark, JavaSparkContext sc) {

        Person row1 = new Person("hayoon", 7, "student");
        Person row2 = new Person("sunwoo", 13, "student");
        Person row3 = new Person("hajoo", 5, "kindergartener");
        Person row4 = new Person("jinwoo", 13, "student");

        List<Person> data = Arrays.asList(row1, row2, row3, row4);

        // from RDD. 리플렉션 방식. schema를 일일이 지정하지 않아도 된다.
        JavaRDD<Person> rdd = sc.parallelize(data);
        Dataset<Row> df5 = spark.createDataFrame(rdd, Person.class);

        df5.show();

        // 스키마 지정 방식. createDataFrame()을 이용. DataFrame은 Row타입으로 구성된 dataset.
        StructField sf1 = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructField sf2 = DataTypes.createStructField("age", DataTypes.IntegerType, true);
        StructField sf3 = DataTypes.createStructField("job", DataTypes.StringType, true);
        StructType schema = DataTypes.createStructType(Arrays.asList(sf1, sf2, sf3));
        Row r1 = RowFactory.create("hayoon", 7, "student");
        Row r2 = RowFactory.create("sunwoo", 13, "student");
        Row r3 = RowFactory.create("hajoo", 5, "kindergartener");
        Row r4 = RowFactory.create("jinwoo", 13, "student");
        List<Row> rows = Arrays.asList(r1, r2, r3, r4);
        Dataset<Row> df6 = spark.createDataFrame(rows, schema);
        df6.show();
    }

    public static void runBasicOperations(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
        df.show();
        Row head = df.head();
        System.out.println("head: " + df.head());
        System.out.println("first: " + df.first());
        // 첫 n 개의  Row
        System.out.println("take : " +df.take(3));
        Row[] take = (Row[]) df.take(3);
        for (Row row: take) {
            System.out.println("take : " + row);
        }

        System.out.println("count : " + df.count());

        // collect는 모든 데이터가 드라이버 프로그램의 메모리에 적재되므로 메모리 부족 에러 발생 가능.
        Row[] collect = (Row[]) df.collect();
        for (Row row: collect) {
            System.out.println("collect : " + row);
        }

        df.describe("age").show();

        // persist
        // cache는 persist와 동일한 기능. default값 MEMORY_AND_DISK 사용.
        df.persist(StorageLevel.MEMORY_AND_DISK_2());

        // schema 관련.
        df.printSchema();
        String[] columns = df.columns();
        System.out.println("columns : " + Arrays.toString(columns));
        Tuple2[] types = df.dtypes();
        System.out.println("types : " + Arrays.toString(types));
        System.out.println("schema : " + df.schema());

        // 데이터프레임을 테이블처럼 SQL을 사용해서 처리할 수 있게 등록
        // 이 메서드로 생성된 테이블은 스파크 세션이 유지되는 동안만 유효.
        df.createOrReplaceTempView("users");
        spark.sql("select name, age from users where age > 10").show();

        // explain. 실행계획
        spark.sql("select name, age from users where age > 10").explain();
    }

    /**
     * 비타입 트랜스퍼메이션. Row, Column, functions를 이해.
     */
    public static void runColumnEx(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
        df.createOrReplaceTempView("person");
        spark.sql("select * from person where age > 10").show();

        System.out.println("---- untyped ----");
        // 비타입
        df.where(col("age").gt(10)).show();
        //df.where("age > 10").show();

        df.where(col("age").equalTo(7)).show();
        df.where(col("name").notEqual("hayoon")).show();
    }

    /**
     * Alias setting. (Columns)
     */
    public static void runAliasEx(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {

        df.select(df.col("age")).show();
        df.select(df.col("age").gt(10)).show();
        df.select(df.col("age").plus(1).as("age_plus")).show();
    }

    /**
     * IsIn (Columns)
     */
    public static void runIsIn(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
        Broadcast<List<Integer>> nums = sc.broadcast(Arrays.asList(1, 3, 5, 7, 9));
        Dataset<Long> ds = spark.range(0, 10);
        ds.where(ds.col("id").isin(nums.value().toArray())).show();
    }

    /**
     * When(if-else) 구문. (functions) 최초로 when을 사용할 때는 functions에서 제공하는 API를 사용하고,
     * 이렇게 생성된 Column 객체에는 dataframe에서 제공하는 when API 를 사용??
     */
    public static void runWhen(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
        Dataset<Long> ds = spark.range(0, 5);
        Column col = when(ds.col("id").mod(2).equalTo(0), "even").otherwise("odd");
        ds.select(ds.col("id"), ds.col("id").mod(2).as("mod"),  col.as("type")).show();
    }

    /**
     * (functions). min, max.
     * @param spark
     * @param df
     */
    public static void runMaxMin(SparkSession spark, Dataset<Row> df) {
        Column minCol = min("age");
        Column maxCol = max("age");
        df.select(minCol, maxCol, mean("age")).show();
    }

    /**
     * (functions).
     * collect_list, collect_set, count, countDistinct, grouping, grouping_id
     */
    public static void runAggregationFunctions(SparkSession spark, Dataset<Row> df1, Dataset<Row> df2) {
        // union
        Dataset<Row> doubledDF1 = df1.union(df1);
        doubledDF1.show();

        // collect_list, collect_set
        //doubledDF1.select(collect_list("name")).show(false);
        //doubledDF1.select(collect_set("name")).show(false);

        // count, countDistinct
        //doubledDF1.select(count("name"), countDistinct("name")).show(false);

        // sum
        df2.select(sum("price")).show(false);

        System.out.println("---- df2 ------");
        df2.show();
        // grouping. grouping 기준의 column의 포함 여부에 따라 null 출력.
        df2.cube(df2.col("store"), df2.col("product")).agg(sum("amount"), grouping("store")).show();
        // grouping_id. grouping level(depth) 에 따른 id 부여.
        Seq<String> colNames = JavaConversions.asScalaBuffer(Arrays.asList("product")).toSeq();
        df2.cube(df2.col("store"), df2.col("product")).agg(sum("amount"), grouping_id("store", colNames)).show();

    }





    


}
