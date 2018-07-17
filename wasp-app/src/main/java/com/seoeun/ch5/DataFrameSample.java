package com.seoeun.ch5;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.api.Annotations;

import javax.xml.crypto.Data;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * DataFrame은 {@linkplain org.apache.spark.sql.Row} 타입의 {@linkplain org.apache.spark.sql.Dataset}을 말한다.
 *
 * @author seoeun
 * @since 2017.10.04
 */
public class DataFrameSample {

    public static void main(String... args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameSample")
                .master("local[*]")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // sample dataframe
        Person row1 = new Person("hayoon", 7, "student");
        Person row2 = new Person("sunwoo", 13, "student");
        Person row3 = new Person("hajoo", 5, "kindergartener");
        Person row4 = new Person("jinwoo", 13, "student");
        Person row5 = new Person("hehe", 8, "student");


        List<Person> data = Arrays.asList(row1, row2, row3, row4, row5);
        Dataset<Row> sampleDf = spark.createDataFrame(data, Person.class);

        Product d1 = new Product("store2", "note", 20, 2000);
        Product d2 = new Product("store2", "bag", 10, 5000);
        Product d3 = new Product("store1", "note", 15, 1000);
        Product d4 = new Product("store1", "pen", 20, 5000);
        Product d5 = new Product("store1", "pen", 13, 150);

        Dataset<Row> sampleDf2 = spark.createDataFrame(Arrays.asList(d1, d2, d3, d4, d5), Product.class);

        Dataset<Row> ldf = spark.createDataFrame(Arrays.asList(new Word("w1", 1), new Word("w2", 1)), Word.class);
        Dataset<Row> rdf = spark.createDataFrame(Arrays.asList(new Word("w1", 2), new Word("w3", 1)), Word.class);

        //createDataFrame(spark, new JavaSparkContext(spark.sparkContext()));

        //runBasicOpsEx(spark, sc, sampleDf);
        //runColumnEx(spark, sc, sampleDf);
        //runAliasEx(spark, sc, sampleDf);
        //runIsinEx(spark, sc, sampleDf);
        //runWhenEx(spark, sc, sampleDf);
        //runMaxMin(spark, sc, sampleDf);
        //runAggregateFunctions(spark, sc, sampleDf, sampleDf2);
        //runCollectionFunctions(spark);
        //runCollectionFunctions2(spark);
        //runDateFunctions(spark);
        //runMathFunctions(spark);
        //runOtherFunctions(spark, sampleDf);
        //runRownumRank(spark);
        //runUDF(spark, sampleDf);
        //runSelectWhere(spark, sampleDf);
        //runAgg(spark, sampleDf2);
        //runDfAlias(spark, sampleDf2);
        //runGroupBy(spark, sampleDf2);
        //runCube(spark, sampleDf2);
        //runRollup(spark, sampleDf2);
        //runDistinct(spark);
        //runDrop(spark, sampleDf2);
        //runIntersectExcept(spark);
        //runJoin(spark, ldf, rdf);
        //runNa(spark, ldf, rdf);
        //runOrderBy(spark);
        //runStat(spark);
        //runWithColumn(spark);
        runSave(spark);


        spark.stop();
    }

    public static void createDataFrame(SparkSession spark, JavaSparkContext sc) {
        String sparkHomeDir = "/usr/lib/spark/";

        // 1. 파일로부터 생성
        Dataset<Row> df1 = spark.read().json(sparkHomeDir + "/examples/src/main/resources/people.json");
        Dataset<Row> df2 = spark.read().parquet(sparkHomeDir + "/examples/src/main/resources/users.parquet");
        Dataset<Row> df3 = spark.read().text(sparkHomeDir + "/examples/src/main/resources/people.txt");

        // 2. Collection 으로부터 생성
        Person row1 = new Person("hayoon", 7, "student");
        Person row2 = new Person("sunwoo", 13, "student");
        Person row3 = new Person("hajoo", 5, "kindergartener");
        Person row4 = new Person("jinwoo", 13, "student");

        List<Person> data = Arrays.asList(row1, row2, row3, row4);
        Dataset<Row> sampleDf = spark.createDataFrame(data, Person.class);

        // 3. RDD 로부터 생성
        JavaRDD<Person> rdd = sc.parallelize(data);
        Dataset<Row> df5 = spark.createDataFrame(rdd, Person.class);
        df5.show();

        // 4. schema 지정
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

    public static void runBasicOpsEx(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
        // default = 20
        df.show();
        df.head();
        System.out.println(df.first());
        Row[] takes = (Row[]) df.take(2);
        for (Row row : takes) {
            System.out.println(row);
        }
        // TODO type cast (scala array -> java array)
        Arrays.asList((Row[]) df.take(2)).stream().forEach((Row row) -> System.out.println(row));
        // For java API,
        df.takeAsList(2).stream().forEach((Row row) -> System.out.println(row.get(0)));

        System.out.println("count = " + df.count());

        System.out.println(" aa = " + df.collectAsList().size());

        // dataFrame의 모든 데이터를 driver 프로그램의 메모리에 적재.
        df.collectAsList().stream().forEach(row -> System.out.println(row));

        // 숫자형 컬럼에 대한 기초 통계값. 반환 타입이 데이터프레임.
        df.describe("age").show();

        // persist를 하면 unpersist 할 때 까지 메모리에 가지고 있거나, LRU에 의해서 제거.
        df.persist(StorageLevel.MEMORY_AND_DISK_2());
        //df.unpersist();

        df.printSchema();
        // scala의 Array[String]은 바로 자바로 변환된다. (primitive 도)
        Arrays.asList(df.columns()).stream().forEach((String column) -> System.out.println(column));

        Arrays.asList(df.dtypes()).stream().forEach((Tuple2 tuple) -> System.out.println(tuple._1() + " = " + tuple._2()));

        // temp view 를 만들어서 sql로 select 가능
        df.createOrReplaceTempView("users");
        spark.sql("select name, age from users where age > 10 ").show();
        // df 를 persist 하냐 안하냐에 따라 plan이 변경됨.
        spark.sql("select name, age from users where age > 10 ").explain();

    }

    /**
     * column 조건 적용
     */
    public static void runColumnEx(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
        // age > 10 인 데이터 조회
        df.createOrReplaceTempView("person");
        // sql
        spark.sql(" select * from person where age > 10").show();
        // api
        df.where(df.col("age").gt(10)).show();

        // alias 적용은 select 만 가능. 
        df.where(df.col("age").equalTo(13).as("age_13")).show();
    }

    /**
     * select 할 때 alias Expression 적용
     */
    public static void runAliasEx(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
        // select 할 때 alias 적용.
        df.select(df.col("age").plus(1)).show();
        df.select(df.col("age").plus(1).as("age_plus")).show();
    }

    /**
     * where 조건에 isin(Array) Expression 적용
     */
    public static void runIsinEx(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
        // where 조건에 isin(Array) 적용.
        Broadcast<List<Integer>> nums = sc.broadcast(Arrays.asList(5, 13));
        //df.where(df.col("age").isin(new Integer[]{5, 13})).show();
        //df.where(df.col("age").isin(5, 13)).show();
        df.where(df.col("age").isin(nums.value().toArray())).show();

        Broadcast<List<Integer>> nums2 = sc.broadcast(Arrays.asList(1, 3, 5, 7, 9));
        Dataset<Long> ds = spark.range(0, 10);
        ds.where(ds.col("id").isin(nums2.value().toArray())).show();
    }

    /**
     * {@link org.apache.spark.sql.functions#when} 구문 적용. IfElse 구문.
     * Column 객체에도 when() 있음.
     */
    public static void runWhenEx(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
        Column col = when(df.col("age").divide(2).equalTo(0), "even").otherwise("odd");
        df.select(df.col("age"), col.as("age_type")).show();
    }

    /**
     * {@link org.apache.spark.sql.functions#min}, {@link org.apache.spark.sql.functions#max},
     * {@link org.apache.spark.sql.functions#mean} 의 집계 함수 사용
     */
    public static void runMaxMin(SparkSession spark, JavaSparkContext sc, Dataset<Row> df) {
        df.select(min("age"), max("age"), mean("age")).show();
    }

    /**
     * AggregationFunctions. collect_list, collect_set, count, sum, cube.
     */
    public static void runAggregateFunctions(SparkSession spark, JavaSparkContext sc,
                                             Dataset<Row> df, Dataset<Row> df2) {
        Dataset<Row> unioned = df.union(df);
        // 특정 column 의 값을 모아서 list 로 된 컬럼을 생성. 중복 허용
        unioned.select(collect_list(col("name"))).show(false);
        // 특정 column 의 값을 모아서 set 로 된 컬럼을 생성. 중복 불가
        unioned.select(collect_set(col("name"))).show(false);

        // 특정 column에 속한 데이터의 갯수를 계산.
        unioned.select(count(col("name")), countDistinct(col("name"))).show();

        // sum
        df2.select(sum(col("price"))).show(false);

        // grouping. 소계를 구할 때 사용. cube
        df2.cube(df2.col("store"), df2.col("product")).agg(sum("amount"), grouping("store")).show();

        // grouping_id
        Seq<String> colNames = JavaConversions.asScalaBuffer(Arrays.asList("product")).toSeq();
        df2.cube(df2.col("store"), df2.col("product")).agg(sum("amount"), grouping_id("store", colNames)).show();
    }

    /**
     * ArrayType의 column에 적용
     * array_contains, size, sort_array
     */
    public static void runCollectionFunctions(SparkSession spark) {
        StructField f1 = DataTypes.createStructField("numbers", DataTypes.StringType, true);
        StructType schema = DataTypes.createStructType(Arrays.asList(f1));
        Row r1 = RowFactory.create("9,1,5,3,9");

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1), schema);
        df.show();
        Column arrayCol = split(df.col("numbers"), ",").as("array_col");
        df.select(arrayCol, array_contains(arrayCol, 2), size(arrayCol)).show();

        // sort_array()
        df.select(arrayCol, sort_array(arrayCol)).show();

        // explode
        df.select(explode(arrayCol)).show(false);
        df.select(posexplode(arrayCol)).show(false);

    }

    /**
     * ArrayType의 column에 대한 연산.
     * array_contains, size, sort_array, explode, posexplode
     */
    public static void runCollectionFunctions2(SparkSession spark) {
        System.out.println("-------------");
        StructField f1 = DataTypes.createStructField("name", DataTypes.StringType, false);
        StructField f2 = DataTypes.createStructField("numbers", DataTypes.createArrayType(DataTypes.StringType), true);
        StructType schema = DataTypes.createStructType(Arrays.asList(f1, f2));
        Row r1 = RowFactory.create("name-01", Arrays.asList("9", "1", "3"));
        Row r2 = RowFactory.create("name-02", Arrays.asList("4", "2", "3"));


        Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1, r2), schema);
        df.show();
        //Column arrayCol = split(df.col("numbers"), ",").as("array_col");
        Column arrayCol = df.col("numbers");
        df.select(col("name"), arrayCol, array_contains(arrayCol, 2), size(arrayCol)).show();

        // sort_array
        df.select(col("name"), arrayCol, sort_array(arrayCol)).show();

        // explode
        df.select(col("name"), arrayCol, explode(arrayCol)).show();
        df.select(col("name"), arrayCol, posexplode(arrayCol)).show();
    }

    /**
     * Date 관련 functions.
     * {@link org.apache.spark.sql.functions#window}를 사용하여 time window 별 group by, aggregation.
     */
    public static void runDateFunctions(SparkSession spark) {
        StructField f1 = DataTypes.createStructField("d1", DataTypes.StringType, true);
        StructField f2 = DataTypes.createStructField("d2", DataTypes.StringType, true);
        StructType schema = DataTypes.createStructType(Arrays.asList(f1, f2));
        Row r1 = RowFactory.create("2017-12-25 12:00:05", "2017-12-25");
        Row r2 = RowFactory.create("2017-10-04 10:47:20", "2017-10-04");

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1, r2), schema);
        df.show();

        Column d3 = current_date().as("d3");
        Column d4 = unix_timestamp(df.col("d1")).as("d4");
        Column d5 = to_date(df.col("d2")).as("d5");
        Column d6 = d4.cast(DataTypes.TimestampType).as("d6");
        Column d11 = date_format(d6, "yyyyMMdd HH:mm:ss").as("d11");
        Dataset<Row> a1 = df.select(df.col("d1"), df.col("d2"), d3, d4, d5, d6, d11);
        a1.printSchema();
        a1.show(false);
        System.out.println("d3 = " + d3.desc());
        System.out.println("d4 = " + d4.desc());
        System.out.println("d5 = " + d5.desc());
        System.out.println("d6 = " + d6.desc());

        Column d7 = add_months(d6, 2).as("d7");
        Column d8 = date_add(d6, 2).as("d8");
        Column d9 = last_day(d6).as("d9-lastday");
        df.select(df.col("d1"), df.col("d2"), d7, d8, d9).show();

        // window
        StructField f3 = DataTypes.createStructField("date", DataTypes.StringType, true);
        StructField f4 = DataTypes.createStructField("product", DataTypes.StringType, true);
        StructField f5 = DataTypes.createStructField("amount", DataTypes.IntegerType, true);
        StructType schema2 = DataTypes.createStructType(Arrays.asList(f3, f4, f5));

        Row r22 = RowFactory.create("2017-12-25 12:01:00", "note", 1000);
        Row r3 = RowFactory.create("2017-12-25 12:01:10", "pencil", 3500);
        Row r4 = RowFactory.create("2017-12-25 12:03:20", "pencil", 23000);
        Row r5 = RowFactory.create("2017-12-25 12:05:00", "note", 1500);
        Row r6 = RowFactory.create("2017-12-25 12:05:07", "note", 2000);
        Row r7 = RowFactory.create("2017-12-25 12:06:25", "note", 1000);
        Row r8 = RowFactory.create("2017-12-25 12:08:00", "pencil", 500);
        Row r9 = RowFactory.create("2017-12-25 12:09:45", "note", 30000);

        Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(r22, r3, r4, r5, r6, r7, r8, r9), schema2);
        df2.show();

        Column timestampCol = unix_timestamp(df2.col("date")).cast("timestamp");
        // 5분 window 생성
        Column windowCol = window(timestampCol, "5 minutes");
        df2.groupBy(windowCol, df2.col("product")).agg(sum(df2.col("amount"))).show(false);

    }

    /**
     * Math functions. round, sqrt.
     */
    public static void runMathFunctions(SparkSession spark) {
        StructField f1 = DataTypes.createStructField("value", DataTypes.DoubleType, true);
        StructType schema1 = DataTypes.createStructType(Arrays.asList(f1));

        Row r1 = RowFactory.create(1.512);
        Row r2 = RowFactory.create(2.234);
        Row r3 = RowFactory.create(3.42);
        Row r4 = RowFactory.create(25.0);
        Row r5 = RowFactory.create(9.0);
        Row r6 = RowFactory.create(10.0);

        Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(r1, r2, r3), schema1);
        Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(r4, r5, r6), schema1);

        df1.select(round(col("value"), 1)).show();
        df2.select(sqrt(col("value"))).show();

    }

    /**
     * array, desc, asc, asc_nulls_first, split, legnth
     */
    public static void runOtherFunctions(SparkSession spark, Dataset<Row> personDf) {
        StructField f1 = DataTypes.createStructField("c1", DataTypes.StringType, true);
        StructField f2 = DataTypes.createStructField("c2", DataTypes.StringType, true);
        StructField f3 = DataTypes.createStructField("c3", DataTypes.StringType, true);
        StructType schema = DataTypes.createStructType(Arrays.asList(f1, f2, f3));
        Row r1 = RowFactory.create("v1", "v2", "v3");
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1), schema);

        // array
        Dataset<Row> newDf = df.select(df.col("c1"), df.col("c2"), df.col("c3"),
                array("c1", "c2", "c3").as("new_array_col"));
        newDf.printSchema();
        newDf.show();
        // save
        newDf.write().json(new File("").getAbsolutePath() + "/tmp/array-test-" + System.currentTimeMillis());

        // desc, asc
        personDf.printSchema();
        personDf.show();
        personDf.sort(desc("age"), asc("name")).show();

        // asc_nulls_first
        Row r2 = RowFactory.create("r11", "r12", "r13");
        Row r3 = RowFactory.create("r21", "r22", null);
        Row r4 = RowFactory.create("r31", "r32", "r33");
        Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(r2, r3, r4), schema);
        df2.show();
        df2.sort(asc_nulls_first("c3")).show();

        // split, length
        StructField f4 = DataTypes.createStructField("value", DataTypes.StringType, true);
        StructType schema2 = DataTypes.createStructType(Arrays.asList(f4));
        Row r5 = RowFactory.create("Splits str around pattern");
        Dataset<Row> df3 = spark.createDataFrame(Arrays.asList(r5), schema2);
        Dataset df3_1 = df3.select(df3.col("value"), split(col("value"), " ").as("s_array"),
                length(col("value")).as("length"));
        df3_1.printSchema();
        df3_1.show(false);
        df3_1.select(df3_1.col("value"), explode(col("s_array"))).show(false);
    }

    /**
     * WindowSpec을 사용해서
     * {@link org.apache.spark.sql.functions#row_number},
     * {@link org.apache.spark.sql.functions#rank}
     */
    public static void runRownumRank(SparkSession spark) {
        StructField f5 = DataTypes.createStructField("date", DataTypes.StringType, true);
        StructField f6 = DataTypes.createStructField("product", DataTypes.StringType, true);
        StructField f7 = DataTypes.createStructField("amount", DataTypes.IntegerType, true);
        StructType schema = DataTypes.createStructType(Arrays.asList(f5, f6, f7));

        Row r6 = RowFactory.create("2017-12-25 12:01:00", "note", 1000);
        Row r7 = RowFactory.create("2017-12-25 12:01:10", "pencil", 3500);
        Row r8 = RowFactory.create("2017-12-25 12:03:20", "pencil", 23000);
        Row r9 = RowFactory.create("2017-12-25 12:05:00", "note", 1500);
        Row r10 = RowFactory.create("2017-12-25 12:05:07", "note", 2000);
        Row r11 = RowFactory.create("2017-12-25 12:06:25", "note", 1000);
        Row r12 = RowFactory.create("2017-12-25 12:08:00", "pencil", 500);
        Row r13 = RowFactory.create("2017-12-25 12:09:45", "note", 30000);

        Dataset<Row> dd = spark.createDataFrame(Arrays.asList(r6, r7, r8, r9, r10, r11, r12, r13), schema);

        WindowSpec w1 = Window.partitionBy(col("product")).orderBy(col("amount").desc());
        WindowSpec w2 = Window.orderBy("amount");

        dd.select(dd.col("product"), dd.col("amount"), row_number().over(w1).as("rownum"),
                rank().over(w2).as("rank"), dd.col("date")).show();

        dd.select(dd.col("product"), dd.col("amount"), row_number().over(w1).as("rownum"),
                rank().over(w2).as("rank")).show();
    }

    /**
     * UDF 사용법.
     */
    public static void runUDF(SparkSession spark, Dataset<Row> df) {
        spark.udf().register("fn2", new UDF1<String, Boolean>() {
            @Override
            public Boolean call(String job) throws Exception {
                return StringUtils.equals(job, "student");
            }
        }, DataTypes.BooleanType);

        spark.udf().register("fn3", (UDF1<String, Boolean>) job -> StringUtils.equals(job, "student"),
                DataTypes.BooleanType);

        df.select(df.col("name"), df.col("age"), df.col("job"), callUDF("fn3", df.col("job"))).show();
        df.createOrReplaceTempView("persons");
        spark.sql("select name, age, job, fn3(job) from persons").show();
    }

    /**
     * select는 데이터프레임의 특정 컬럼만 포함된 새로운 데이터 프레임을 생성.
     * where은 데이트프레임의 레코드중에서 특정 조건을 만족하는 레코드만 선택. filter와 동일.
     */
    public static void runSelectWhere(SparkSession spark, Dataset<Row> df) {
        // select
        df.select(col("name"), col("age")).show();
        df.select(col("name"), col("age")).where(col("age").gt(10)).show();

        df.where(col("age").gt(10)).show();
    }

    /**
     * {@link org.apache.spark.sql.Dataset#agg}를 이용하여 aggregation.
     * 인자로 functions#max 와 같은 expression 사용.
     */
    public static void runAgg(SparkSession spark, Dataset<Row> df) {
        df.show();
        df.agg(max("amount"), min("price")).show();

        // key=column 명, value=expression(avg, max, min, sum, count)
        Map<String, String> params = new HashMap<>();
        params.put("amount", "max");
        params.put("price", "min");
        df.agg(params).show();
    }

    /**
     * {@link org.apache.spark.sql.Dataset#alias}를 사용하여 Dataframe에 alias를 부여.
     */
    public static void runDfAlias(SparkSession spark, Dataset<Row> df) {
        df.select(df.col("product")).show();
        df.alias("aa").select("aa.product").show();
    }

    /**
     * {@link org.apache.spark.sql.Dataset#groupBy}. agg와 함께 사용.
     */
    public static void runGroupBy(SparkSession spark, Dataset<Row> df) {
        df.groupBy("store", "product").agg(sum("price")).show();

        df.show();
        // map 사용
        Map<String, String> params = new HashMap<>();
        params.put("price", "sum");
        params.put("price", "min");  // key가 같아서 min만 적용됨.
        df.groupBy("store", "product").agg(params).show();

        df.groupBy("store", "product").agg(sum("price"), min("price")).show();

    }

    /**
     * Dataframe의  cube를 사용하면 소계를 알 수 있다. grouping_id와 함께 사용.
     */
    public static void runCube(SparkSession spark, Dataset<Row> df) {
        df.cube("store", "product").agg(sum("price")).show();

        // grouping. 소계를 구할 때 사용. cube
        df.cube(df.col("store"), df.col("product")).agg(sum("amount"), grouping("store")).show();

        // grouping_id. grouping_id를 사용하면 소계 단계를 알 수 있다.
        Seq<String> colNames = JavaConversions.asScalaBuffer(Arrays.asList("product")).toSeq();
        df.cube(df.col("store"), df.col("product")).agg(sum("amount"), grouping_id("store", colNames)).show();
    }

    /**
     * Dataframe의 rollup. cube와 사용법이 같음.
     */
    public static void runRollup(SparkSession spark, Dataset<Row> df) {
        System.out.println("--------- rollup ------");
        df.rollup("store", "product").agg(sum("amount")).show();
        df.rollup("store", "product").agg(ImmutableMap.of("amount", "sum")).show();

        // grouping_id
        Seq<String> colNames = JavaConversions.asScalaBuffer(Arrays.asList("product")).toSeq();
        df.rollup("store", "product").agg(sum("amount"), grouping_id("store", colNames).as("step"))
                .orderBy("store", "step").show();

    }

    /**
     * Dataframe의 distinct는 모든 컬럼의 값이 같을 때만 중복이라고 판단.
     * dropDuplicates는 특정 컬럼의 값이 같을 때 중복이라고 판단.
     */
    public static void runDistinct(SparkSession spark) {
        Product d1 = new Product("store1", "note", 20, 2000);
        Product d2 = new Product("store1", "bag", 10, 5000);
        Product d3 = new Product("store1", "note", 20, 2000);
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(d1, d2, d3), Product.class);
        df.distinct().show();
        df.dropDuplicates("store").show();
    }

    /**
     * Dataframe의 drop은 특정 컬럼을 제외.
     */
    public static void runDrop(SparkSession spark, Dataset<Row> df) {
        df.drop(col("store")).show();
    }

    /**
     * Dataframe의 intersect는 두 개의 데이터프레임에 모두 속하는 로우로만 구성된 데이터프레임을 생성. 교집합.
     * except 는 하나의 데이터프레임에 속하고 다른 데이터프레임에는 속하지 않는 로우.
     * 합집합에서 교집합을 뺀 것.
     */
    public static void runIntersectExcept(SparkSession spark) {
        Dataset<Row> df1 = spark.range(1, 5).toDF();
        df1.show(); // 1,2,3,4
        Dataset<Row> df2 = spark.range(2, 4).toDF();
        df2.show(); //2,3
        Dataset<Row> c = df1.intersect(df2);
        c.show();
        // 2, 3

        Dataset<Row> d = df1.except(df2);
        d.show();
        // 1, 4
    }

    /**
     * Dataframe의 join.
     * inner, outer, left_outer, right_outer, leftsemi
     */
    public static void runJoin(SparkSession spark, Dataset<Row> ldf, Dataset<Row> rdf) {
        // inner
        System.out.println("----- inner -----");
        ldf.join(rdf, ldf.col("word").equalTo(rdf.col("word")), "inner").show();
        System.out.println("---- inner join not duplicated column ----");
        Seq<String> joinCols = JavaConversions.asScalaBuffer(Arrays.asList("word")).toSeq();
        ldf.join(rdf, joinCols, "inner").show();

        ldf.join(rdf, joinCols, "inner").select(col("word"), ldf.col("count").as("l_count"), rdf.col("count").as
                ("r_count")).show();

        System.out.println("---- outer == full outer ----");
        ldf.join(rdf, joinCols, "outer").select(col("word"), ldf.col("count").as("l_count"),
                rdf.col("count").as("r_count")).show();

        System.out.println("---- left_outer ----");
        ldf.join(rdf, joinCols, "left_outer").select(col("word"), ldf.col("count").as("l_count"),
                rdf.col("count").as("r_count")).show();

        System.out.println("---- right_outer ----");
        ldf.join(rdf, joinCols, "right_outer").select(col("word"), ldf.col("count").as("l_count"),
                rdf.col("count").as("r_count")).show();

        System.out.println("---- leftsemi ----");
        ldf.join(rdf, joinCols, "leftsemi").show();

        System.out.println("---- cartesian ----");
        ldf.crossJoin(rdf).show();

    }

    /**
     * Dataframe의 특정 컬럼이 null 일 때 처리.
     */
    public static void runNa(SparkSession spark, Dataset<Row> ldf, Dataset<Row> rdf) {
        // drop
        ldf = ldf.toDF("c1", "word");
        rdf = rdf.toDF("c2", "word");
        ldf.show();
        rdf.show();
//        Dataset<Row> result = ldf.join(rdf, ldf.col("lword").equalTo(rdf.col("rword")), "outer")
//                .select(ldf.col("lword").as("word"), ldf.col("c1"), rdf.col("c2"));
//
        Seq<String> joinCols = JavaConversions.asScalaBuffer(Arrays.asList("word")).toSeq();
        Dataset<Row> result = ldf.join(rdf, joinCols, "outer");
        result.show();

        // drop : c1, c2중에 null이 아닌 필드가 최소 2개
        result.na().drop(2, new String[]{"c1", "c2"}).show();

        // fill
        Map<String, Object> fillMap1 = new HashMap<>();
        fillMap1.put("c1", 0);
        result.na().fill(fillMap1).show();
        
        // replace . null 이 아니어도 가능.
        Map<String, String> fillMap2 = new HashMap<>();
        fillMap2.put("w1", "word1_null");
        fillMap2.put("w2", "word2_null");
        result.na().replace("word", fillMap2).show();

    }

    /**
     * Dataframe의 orderBy. sort의 alias
     */
    public static void runOrderBy(SparkSession spark) {
        StructField f1 = DataTypes.createStructField("idx", DataTypes.IntegerType, true);
        StructField f2 = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructType schema1 = DataTypes.createStructType(Arrays.asList(f1, f2));

        Row r1 = RowFactory.create(3, "c");
        Row r2 = RowFactory.create(10, "a");
        Row r3 = RowFactory.create(5, "z");

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1, r2, r3), schema1);
        df.orderBy("name", "idx").show();
        df.orderBy("idx", "name").show();
    }

    /**
     * Dataframe에서 특정 컬럼에 대해서 자주 사용하는 통계 값을 function으로 제공.
     * 상관계수-corr, 공분산-cov, 크로스탭-crosstab, freqItems, sampleBy
     * @param spark
     */
    public static void runStat(SparkSession spark) {
        StructField f1 = DataTypes.createStructField("word", DataTypes.StringType, true);
        StructField f2 = DataTypes.createStructField("count", DataTypes.IntegerType, true);
        StructType schema = DataTypes.createStructType(Arrays.asList(f1, f2));

        Row r1 = RowFactory.create("a", 6);
        Row r2 = RowFactory.create("b", 4);
        Row r3 = RowFactory.create("c", 12);
        Row r4 = RowFactory.create("d", 6);

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(r1, r2, r3, r4), schema);
        df.show();
        df.stat().crosstab("word", "count").show();
    }

    /**
     * Dataframe에 새로운 컬럼을 추가하거나, 기존 컬럼의 이름을 변경
     */
    public static void runWithColumn(SparkSession spark) {
        StructField f1 = DataTypes.createStructField("pname", DataTypes.StringType, true);
        StructField f2 = DataTypes.createStructField("price", DataTypes.StringType, true);
        StructType schema1 = DataTypes.createStructType(Arrays.asList(f1, f2));

        Row r1 = RowFactory.create("prod1", "100");
        Row r2 = RowFactory.create("prod2", "200");
        Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(r1, r2), schema1);
        Dataset<Row> df2 = df1.withColumn("dprice", df1.col("price").multiply(0.9));
        Dataset<Row> df3 = df2.withColumnRenamed("dprice", "newprice");

        df1.show();
        df2.show();
        df3.show();
    }

    public static void runSave(SparkSession spark) {
        String sparkHomeDir = "/usr/lib/spark/";

        // 1. 파일로부터 생성
        Dataset<Row> df1 = spark.read().json(sparkHomeDir + "/examples/src/main/resources/people.json");
        Dataset<Row> df2 = spark.read().parquet(sparkHomeDir + "/examples/src/main/resources/users.parquet");
        Dataset<Row> df3 = spark.read().text(sparkHomeDir + "/examples/src/main/resources/people.txt");

        String saveRoot = new File("").getAbsolutePath();
        System.out.println("saveRoot = " + saveRoot);
        File tmp = new File(saveRoot + "/tmp");
        if (!tmp.exists()) {
            tmp.mkdir();
        }

        df1.show();
        df1.write().save(tmp.getAbsolutePath() + "/people-" + System.currentTimeMillis());
        df2.write().json(tmp.getAbsolutePath() + "/users-" + System.currentTimeMillis());

        df3.show();
        // save as hive table
        // df3.write().saveAsTable("save-as-hive");

    }


}
