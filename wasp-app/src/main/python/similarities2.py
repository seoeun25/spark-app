import itertools
import sys
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions
#from pyspark.sql import functions as F

# spark-submit wikibook/src/main/python/similarities.py azra brook01.table

def dataLoad(spark, locale="ko-KR"):
    """
    queryStr = "SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity WHERE locale='{}'" \
               "".format(locale)
    """

    #"""
    queryStr = "SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity WHERE locale='{}'" \
               " and " \
               "content_id IN (1, 2, 3) ".format(locale)
    #"""

    print(queryStr)
    df_load = spark.sql(queryStr)
    # df_load = spark.sql('select user_id, content_id, locale, adult, purchase_cnt from
    # actdb.purchase_count_similarity limit 10')
    # df_load.show()

    print("----- {}".format(df_load.head(5)))
    print("---- rdd = {}".format(df_load.rdd))

    df_load.printSchema()
    print(" --- row count = {}".format(df_load.count()))

    df2 = df_load.select(
        df_load["user_id"],
        df_load["content_id"],
        functions.when(df_load["purchase_cnt"] >= 10, 10).otherwise(df_load["purchase_cnt"]).alias("score"))
    print(" --- df2 = {}".format(df2))
    df2.show()


    dfInfoDic = df2.groupBy("content_id").sum("score")
    print(" --- dfInfoDic = {}".format(dfInfoDic))
    dfInfoDic.show()

    dfUser = df_load.select(df_load["user_id"], functions.create_map('content_id', 'purchase_cnt').alias("cmap"))
    #dfUser.show()

    dfUser2 = dfUser.groupby("user_id").agg(functions.collect_list("cmap").alias("content_map"))
    dfUser2.show()

    dfUserDic = df_load.groupby("user_id").agg(functions.collect_list("content_id").alias("content_ids"),
                                               functions.collect_list("purchase_cnt").alias("purchases"))

    #rdd = dfUserDic.rdd()
    #print(" --- rdd = {}".format(rdd))


    #dfUserDic.show()


    #rowList = df_load.collect()
    """
    rowList = {}
    print(" --- rowList.len = {}".format(rowList.__len__()))

    item_dic = {}
    user_dic = {}
    for i in range(0, rowList.__len__()):
        item = rowList[i]
        #print("--- item {} = {}".format(i, item))
        content_id = item["content_id"]
        user_id = item["user_id"]
        cnt = item["purchase_cnt"]
        if (i == 0) :
            print(" cnt = {}".format(cnt))
            print(cnt)

        item_dic.setdefault(content_id, {})
        item_dic[content_id][user_id] = int("{}".format(cnt))
        user_dic.setdefault(user_id, {})
        user_dic[user_id][content_id] = int("{}".format(cnt))

    print(" -- item_dic len = {}".format(item_dic.__len__()))
    print(" -- user_dic len = {}".format(user_dic.__len__()))
    print(" -- item_dic.key len = {}".format(item_dic.keys().__len__()))
    """

    """
    for key in item_dic.keys():
        #print(" -- key = {}".format(key))
        item2 = item_dic[key]
        #print(" --- itemdic = {}".format(item2))
    """

    #print("df_load = {}".format(df_load))
    print("data load Done!")

    return (dfUser2, dfInfoDic)


# 작품별 총점 계산
"""
def data_cal_info(df):

    result = {}
    for item in item_dic:
        score = 0
        for person in item_dic[item]:
            if item_dic[item][person] >= 10:
                score += 10
            else:
                score += item_dic[item][person]
        result.setdefault(item, {})
        result[item] = score

    #for item in result:
    #    print("item = {}".format(item))

    print("score items = {}".format(result.__len__()))
    return result
"""

# 작품간 스코어 계산
def data_cal_table(user_df, info_df):

    user_dic = user_df.collect()
    item_dic = info_df.collect()

    start_time = time.time()
    result = {}

    # 비어있는 딕셔너리 생성
    for item in item_dic:
        #print(" --- item in item_dic = {}".format(item))
        result.setdefault(item, {})
        for other in item_dic:
            if item != other:
                result[item][other] = 0

    for user in user_dic:
        print(" --- user in user_dic = {}".format(user))
        print(" --- user in user_dic = {}".format(user.content_map))

        # 중복없는 조합을 만들어서 계산량을 최소화 (ver1은 3시간 -> 현재 ver7은 10분)
        for tup in itertools.combinations(user.content_map, 2):
            (item, other) = tup
            print(type(item))
            print(type(tup))
            print(" --- item = {}, ".format(item))

            print(" --- other = {}, other_id = {}".format(other, other[key]))

            for key,val in tup:
                print(key, "=>", val)
            print(" --- item_id = {}".format(item, item[key]))


            # 중복없는 조합이기 때문에 한 번에 쌍으로 저장
            # 구매건수 10건 이상은 10점으로 처리
            if user_df[user][item] >= 10:
                result[item][other] += 10
            else:
                result[item][other] += user_df[user][item]
            if user_df[user][other] >= 10:
                result[other][item] += 10
            else:
                result[other][item] += user_df[user][other]
    end_time = time.time()
    print('---- data_cal_table', int(end_time - start_time), 'sec')
    print("---- result = {}".format(result.keys().__len__()))

    return result

def test_collect_functions(spark):
    print(" ---- test_collect_functions")
    df = spark.createDataFrame([
            ("a", None, None),
            ("a", "code1", None),
            ("a", "code2", "name2"),
            ("b", "code2", "name2"),
            ("b", "code3", "name4"),
        ], ["id", "code", "name"])

    df.show()

    """
    (df
     .groupby("id")
     .agg(F.collect_set("code"),
          F.collect_list("name"))
     .show())
    """
    print(" ---- test_collect_functions. df2")

    df2 = df.groupby("id").agg(functions.collect_set("code"), functions.collect_list("name"))
    df2.show()


    """
    self.assertEqual(
        sorted(df.select(functions.collect_set(df.key).alias('r')).collect()[0].r),
        [1, 2])
    self.assertEqual(
        sorted(df.select(functions.collect_list(df.key).alias('r')).collect()[0].r),
        [1, 1, 1, 2])
    self.assertEqual(
        sorted(df.select(functions.collect_set(df.value).alias('r')).collect()[0].r),
        ["1", "2"])
    self.assertEqual(
        sorted(df.select(functions.collect_list(df.value).alias('r')).collect()[0].r),
        ["1", "2", "2", "2"])
    """

def test_collect_functions2(spark):
    print(" ---- test_collect_functions2")
    df = spark.createDataFrame([
        ("a", "user1", 1),
        ("b", "user1", 2),
        ("b", "user2", 4),
        ("c", "user2", 5),
    ], ["content_id", "user_id", "cnt"])

    df.show()

    """
    (df
     .groupby("id")
     .agg(F.collect_set("code"),
          F.collect_list("name"))
     .show())
    """
    print(" ---- test_collect_functions. df2")

    df2 = df.groupby("user_id").agg(functions.collect_list("content_id"), functions.collect_list("cnt"))
    df2.show()


def data_cal_sim(table_dic, info_dic):
    start_time = time.time()
    result = {}
    for item in table_dic:
        a_score = info_dic[item]
        for other in table_dic[item]:
            b_score = info_dic[other]
            ab_score = table_dic[item][other] + table_dic[other][item]
            score = float(ab_score) / (a_score + b_score)
            result.setdefault(item, {})
            result[item][other] = score

    end_time = time.time()
    print('---- data_cal_sim', int(end_time - start_time), 'sec')
    print("---- result = {}".format(result.keys().__len__()))

    return result

#정규화
def get_scale(df_set):
    for col in df_set.columns:
        df_set[col]=(df_set[col] - df_set[col].min()) / (df_set[col].max() - df_set[col].min())
    for idx in df_set.index:
        df_set.ix[idx]=(df_set.ix[idx] - df_set.ix[idx].min()) / (df_set.ix[idx].max() - df_set.ix[idx].min())
    print('get_scale')
    return df_set


if __name__ == "__main__":
    print(sys.argv)
    print(sys.argv.__len__())
    if len(sys.argv) != 3:
        print("Error usage: LoadHive [sparkmaster] [inputtable]")
        sys.exit(-1)
    master = sys.argv[1]
    inputTable = sys.argv[2]
    print("master: %s", master)
    print("inputTable:%s", inputTable)

    # We have to set the hive metastore uri.
    hiveMetastore = "thrift://azra:9083"
    #hiveMetastore = "thrift://insight-v3-m:9083"

    SparkContext.setSystemProperty("hive.metastore.uris", hiveMetastore)

    # Spark Session and DataFrame creation
    spark = (SparkSession
             .builder
             .appName('test-hive')
             .config("spark.executor.heartbeatInterval", "36000s")
             .enableHiveSupport()
             .getOrCreate())

    interval = spark.sparkContext.getConf().getAll()
    #get("spark.executor.")
    print("-- interval = {}".format(interval))

    # data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
    # df = sparkSession.createDataFrame(data)

    # Write into Hive
    # df.write.saveAsTable('example')

    #test_collect_functions2(spark)


    #데이터 로드
    (user_df,info_df)=dataLoad(spark)

    #인포딕셔너리 생성
    #info_dic=data_cal_info(item_dic)

    #유사도 계산
    table_dic=data_cal_table(user_df, info_df)
    #sim_dic=data_cal_sim(table_dic, info_dic)

    schema = StructType([
        StructField('userId', IntegerType()),
        StructField('movieId', IntegerType()),
        StructField('rating', DoubleType()),
        StructField('timestamp', StringType())
    ])

    #데이터프레임 생성
    #df_set=pd.DataFrame.from_dict(sim_dic)
    #df_set=df_set.fillna(0)
    #df_set=get_scale(df_set)


    print("Done!")
    spark.stop
