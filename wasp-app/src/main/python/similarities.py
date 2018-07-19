import itertools
import sys
import time

import pandas as pd
import numpy as np


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# spark-submit wasp-app/src/main/python/similarities.py azra brook01.table

def dataLoad(spark, locale="ko-KR"):
    #queryStr = "SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity WHERE locale='{}'" \
    #           "".format(locale)

    queryStr = "SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity WHERE locale='{}'" \
               " and " \
               "content_id IN (1, 2, 3, 4) ".format(locale)

    print("  ===  {} ".format(queryStr))
    df_load = spark.sql(queryStr)
    # df_load = spark.sql('select user_id, content_id, locale, adult, purchase_cnt from
    # actdb.purchase_count_similarity limit 10')
    # df_load.show()

    print("----- {}".format(df_load.head(5)))
    print("---- rdd = {}".format(df_load.rdd))

    df_load.printSchema()
    print(" --- row count = {}".format(df_load.count()))

    rowList = df_load.collect()
    print(" --- rowList.len = {}".format(rowList.__len__()))

    item_dic = {}
    user_dic = {}
    for i in range(0, rowList.__len__()):
        item = rowList[i]
        #print("--- item {} = {}".format(i, item))
        content_id = item["content_id"]
        user_id = item["user_id"]
        cnt = item["purchase_cnt"]

        item_dic.setdefault(content_id, {})
        item_dic[content_id][user_id] = int("{}".format(cnt))
        user_dic.setdefault(user_id, {})
        user_dic[user_id][content_id] = int("{}".format(cnt))

    print(" -- item_dic len = {}".format(item_dic.__len__()))
    print(" -- user_dic len = {}".format(user_dic.__len__()))
    print(" -- item_dic.key len = {}".format(item_dic.keys().__len__()))

    """
    for key in item_dic.keys():
        #print(" -- key = {}".format(key))
        item2 = item_dic[key]
        #print(" --- itemdic = {}".format(item2))
    """

    #print("df_load = {}".format(df_load))
    print("data load Done!")

    return (user_dic, item_dic)


# 작품별 총점 계산
def data_cal_info(item_dic):
    print("--- item_dic len = {}".format(item_dic.__len__()))

    result = {}
    for item in item_dic:
        print("--- item in item_dic = {}".format(item))
        score = 0
        for person in item_dic[item]:
            if item_dic[item][person] >= 10:
                #print("---- max10. item= {}, persion={}".format(item, person))
                score += 10
            else:
                score += item_dic[item][person]
        result.setdefault(item, {})
        result[item] = score

    for item in result:
        print("{}, result[{}] = {}".format(item, item, result[item]))

    print("score items = {}".format(result.__len__()))
    return result


# 작품간 스코어 계산
def data_cal_table(user_dic, item_dic):

    start_time = time.time()
    result = {}

    # 비어있는 딕셔너리 생성
    for item in item_dic:
        result.setdefault(item, {})
        for other in item_dic:
            if item != other:
                result[item][other] = 0
                print("00 == result[{}][{}] = {}".format(item, other, result[item][other]))

    print("--- data_cal_table. item_dic len = {}".format(item_dic.__len__()))
    print("--- data_cal_table .user_dic len = {}".format(user_dic.__len__()))

    #for item in result:
        #print("type = {}".format(type(item)))
        #print("item = {}".format(item))


    for user in user_dic:
        #print("--- user in user_dic = {}".format(user))
        # 중복없는 조합을 만들어서 계산량을 최소화 (ver1은 3시간 -> 현재 ver7은 10분)
        #print("user = {}".format(user))
        #print("items = {}".format(user_dic[user].keys()))

        for tup in itertools.combinations(user_dic[user].keys(), 2):
            (item, other) = tup
            print("user={} item={}, other={}".format(user, item, other))
            # 중복없는 조합이기 때문에 한 번에 쌍으로 저장
            # 구매건수 10건 이상은 10점으로 처리
            if user_dic[user][item] >= 10:
                result[item][other] += 10
                print("    [{}][{}] = {}".format(item, other, result[item][other]))
            else:
                result[item][other] += user_dic[user][item]
                print("    [{}][{}] = {}".format(item, other, result[item][other]))
            if user_dic[user][other] >= 10:
                result[other][item] += 10
                print("    [{}][{}] = {}".format(other, item, result[other][item]))
            else:
                result[other][item] += user_dic[user][other]
                print("    [{}][{}] = {}".format(other, item, result[other][item]))
    end_time = time.time()
    print('---- data_cal_table', int(end_time - start_time), 'sec')
    print("---- result = {}".format(result.keys().__len__()))

    for item in result:
        print("item in result = {}".format(item))
        print("item[] in result = {}".format(result[item]))

    return result


def data_cal_sim(table_dic, info_dic):
    start_time = time.time()
    result = {}
    for item in table_dic:
        a_score = info_dic[item]
        for other in table_dic[item]:
            b_score = info_dic[other]
            #print("table_dic[{}][{}] = {}".format(item, other, table_dic[item][other]))
            #print("table_dic[{}][{}] = {}".format(other, item, table_dic[other][item]))

            ab_score = table_dic[item][other] + table_dic[other][item]
            score = float(ab_score) / (a_score + b_score)
            result.setdefault(item, {})
            result[item][other] = score

    end_time = time.time()
    print('---- data_cal_sim', int(end_time - start_time), 'sec')
    print("---- result = {}".format(result.keys().__len__()))

    for item in result:
        print("item in data_cal_sim = {} = {}".format(item, result[item]))


    return result

#정규화
def get_scale(df_set):
    for col in df_set.columns:
        print("col = {}, {}".format(col, df_set[col]))
        df_set[col]=(df_set[col] - df_set[col].min()) / (df_set[col].max() - df_set[col].min())
    for idx in df_set.index:
        print("idx = {}, {}".format(idx, df_set.ix[idx]))
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

    #데이터 로드
    (user_dic,item_dic)=dataLoad(spark)

    #인포딕셔너리 생성
    info_dic=data_cal_info(item_dic)

    #유사도 계산
    table_dic=data_cal_table(user_dic, item_dic)
    sim_dic=data_cal_sim(table_dic, info_dic)

    schema = StructType([
        StructField('userId', IntegerType()),
        StructField('movieId', IntegerType()),
        StructField('rating', DoubleType()),
        StructField('timestamp', StringType())
    ])

    #데이터프레임 생성
    df_set=pd.DataFrame.from_dict(sim_dic)
    print("== dfs_set = {}".format(df_set))
    for item in df_set:
        print("--- dfset item = {} = {}".format(item, df_set[item]))

    df_set=df_set.fillna(0)
    df_set=get_scale(df_set)


    print("Done!")
    spark.stop
