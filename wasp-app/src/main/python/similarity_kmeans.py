import itertools
import sys
import time

import pymysql
from pyhive import presto

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import *

# sudo apt-get install python3-pip
# sudo apt-get install python3-pandas
# sudo apt-get install python3-pymysql
# sudo pip3 install pyhive
# sudo pip install sklearn
# spark-submit wasp-app/src/main/python/similarity_kmeans.py [ymd] [locale] [adult_kind] [metastore]

def dataLoad(spark, locale, adult_kind):
    #queryStr = "SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_agg WHERE locale='{}'" \
    #           "".format(locale)

    queryStr = "SELECT source_content_id, target_content_id, abscore " \
               "FROM actdb.content_similarity_{}{}" \
               " WHERE locale='{}' and adult_kind ='{}'".format(locale[:2], adult_kind, locale, adult_kind)

    print("   ===  queryStr {} ".format(queryStr))
    df_load = spark.sql(queryStr)
    # df_load = spark.sql('select user_id, content_id, locale, adult_kind, purchase_cnt from
    # actdb.purchase_count_agg limit 10')
    # df_load.show()

    print("----- {}".format(df_load.head(5)))
    #print("---- rdd = {}".format(df_load.rdd))

    df_load.printSchema()
    print(" --- row count = {}".format(df_load.count()))

    rowList = df_load.collect()
    print(" --- rowList.len = {}".format(rowList.__len__()))


    score_dic = {}
    for i in range(0, rowList.__len__()):
        item = rowList[i]
        #print("--- item {} = {}".format(i, item))
        source_content_id = item["source_content_id"]
        target_content_id = item["target_content_id"]
        abscore = item["abscore"]

        score_dic.setdefault(target_content_id, {})
        score_dic[target_content_id][source_content_id] = float("{}".format(abscore))

    print(" -- score_dic len = {}".format(score_dic.__len__()))
    print(" -- score_dic.key len = {}".format(score_dic.keys().__len__()))


    #for item in score_dic.keys():
        #print(" --- score_dic: {} = {}".format(item, score_dic[item]))


    #print("df_load = {}".format(df_load))
    print("data load Done!")

    return score_dic

#가중치 조정
#비슷한 작품이 잘 묶일 수 있도록 정규화된 데이터에 가중치를 조정
def get_weight(df_set):
    for idx in df_set.index:
        #최대 가중치 (로케일별로 조정이 필요함)
        p=3
        z_mean=df_set.ix[idx].mean()
        z_std=df_set.ix[idx].std()
        z=z_mean+(z_std*4)

        #가장 유사한 작품 가중치
        df_set.ix[idx][df_set.ix[idx]==1]=p
        #자기 작품 가중치
        df_set[idx][idx]=p
        #상위 작품 가중치
        df_set.ix[idx][(df_set.ix[idx]>z) & (df_set.ix[idx]<p)]+=(df_set.ix[idx][(df_set.ix[idx]>z) & (df_set.ix[idx]<p)]*0.5)

    print("get_weight finish")
    return df_set

def get_cls(df_set,num_cls):
    arr_set=np.array(df_set)
    #print("---- arr_set = \n", arr_set)
    kmeans_result=[]
    print("---- num_cls = \n", num_cls)

    kmeans=KMeans(n_clusters=num_cls)
    kmeans.fit(arr_set)
    kmeans_result=list(kmeans.predict(arr_set))
    print("get_cls finish")
    #print(" ==== kmeans_result ==== \n", kmeans_result)
    print(" ================")
    return kmeans_result

def get_blend(df_set, kmeans_result):
    #유사도 낮은 작품 제외하기
    con_set_dic={}
    set_con_dic={}
    for i in range(0,len(kmeans_result)):
        con_set_dic.setdefault(df_set.index[i],{})
        con_set_dic[df_set.index[i]]=kmeans_result[i]
        set_con_dic.setdefault(kmeans_result[i],[])
        set_con_dic[kmeans_result[i]].append(df_set.index[i])

    for i in range(0,2):
        for j in range(0,5):
            c=0
            new_set=max(set_con_dic)+1
            for set in set_con_dic:
                if len(set_con_dic[set])>1:
                    for seed in set_con_dic[set]:
                        score=0
                        cnt=0
                        for con in set_con_dic[set]:
                            if seed!=con:
                                score+=df_set.ix[seed][con]
                                cnt+=1
                        if score/cnt < 0.6:
                            con_set_dic[seed]=new_set
                            new_set+=1
                            c+=1

            set_con_dic={}
            for seed in con_set_dic:
                set_con_dic.setdefault(con_set_dic[seed],[])
                set_con_dic[con_set_dic[seed]].append(seed)

            print('extra', len(set_con_dic), c)

        #가장 비슷한 세트끼리 묶기
        c=0
        for set in set_con_dic:
            new_set=0
            if len(set_con_dic[set])<6:
                c+=1
                tmp_series=0
                for seed in set_con_dic[set]:
                    tmp_series+=df_set.ix[seed]

                rank_list=[]
                for other_set in set_con_dic:
                    if set!=other_set:
                        score=0
                        for other_seed in set_con_dic[other_set]:
                            score+=tmp_series[other_seed]
                        score=score/len(set_con_dic[other_set])
                        rank_list.append((score,other_set))

                rank_list.sort()
                rank_list.reverse()
                new_set=rank_list[0][1]
                for seed in set_con_dic[set]:
                    con_set_dic[seed]=new_set

                set_con_dic={}
                for seed in con_set_dic:
                    set_con_dic.setdefault(con_set_dic[seed],[])
                    set_con_dic[con_set_dic[seed]].append(seed)

        print('merge', len(set_con_dic), c)

        #평균값 측정
        all_score=0
        for set in set_con_dic:
            for seed in set_con_dic[set]:
                seed_score=0
                for con in set_con_dic[set]:
                    if seed!=con:
                        seed_score+=df_set.ix[seed][con]
                all_score+=seed_score/(len(set_con_dic[set])-1)
        print(i, all_score/len(con_set_dic))
    print("get_blend finish")
    #print(con_set_dic)
    return con_set_dic

def get_media(spark):

    mediaQuery = "SELECT id, title, sum_read_users, sum_purchase_cnt " \
                 "FROM actdb.media_purchase_read_agg"

    print("  mediaQuery ===  {} ".format(mediaQuery))
    mediaDf = spark.sql(mediaQuery)
    mediaList = mediaDf.collect()
    print(" --- rowList.len = {}".format(mediaList.__len__()))


    media_dic={}

    for i in range(0, mediaList.__len__()):
        item = mediaList[i]
        id = item["id"]
        title = item["title"]
        sum_read_users = item["sum_read_users"]
        sum_purchase_cnt = item["sum_purchase_cnt"]

        media_dic.setdefault(id,{})
        media_dic[id]['sum_read_users']=float(sum_read_users)
        media_dic[id]['sum_purchase_cnt']=float(sum_purchase_cnt)

    #print(" ---- media_dic", media_dic)
    print("media_dic finish")
    return media_dic

def get_order(con_set_dic, media_dic):
    #열람 하나, 구매 하나
    rdsum_list=[]
    pursum_list=[]
    for item in con_set_dic:
        if 'sum_read_users' not in media_dic[item]:
            sum_read_users=0
        else:
            sum_read_users=media_dic[item]['sum_read_users']
        rdsum_list.append((sum_read_users,item,con_set_dic[item]))

        if 'sum_pur_cnt' not in media_dic[item]:
            sum_pur_cnt=0
        else:
            sum_pur_cnt=media_dic[item]['sum_pur_cnt']
        pursum_list.append((sum_pur_cnt,item,con_set_dic[item]))

    rdsum_list.sort()
    rdsum_list.reverse()
    pursum_list.sort()
    pursum_list.reverse()

    media_list=[]
    check_list=[]
    for i in range(0,len(rdsum_list)):
        #    (b,pur_content_id,pur_set_id)=pur_list[i]
        #    if pur_content_id not in check_list:
        #        media_list.append((pur_content_id,pur_set_id))
        #        check_list.append(pur_content_id)

        (c,rdsum_content_id,rdsum_set_id)=rdsum_list[i]
        if rdsum_content_id not in check_list:
            media_list.append((rdsum_content_id,rdsum_set_id))
            check_list.append(rdsum_content_id)

            #    (a,rd_content_id,rd_set_id)=rd_list[i]
            #    if rd_content_id not in check_list:
            #        media_list.append((rd_content_id,rd_set_id))
            #        check_list.append(rd_content_id)

        (d,pursum_content_id,pursum_set_id)=pursum_list[i]
        if pursum_content_id not in check_list:
            media_list.append((pursum_content_id,pursum_set_id))
            check_list.append(pursum_content_id)

    rank_set_list=[]
    rank_con_dic={}
    for i in range(0,len(media_list)):
        (content_id,set_id)=media_list[i]
        if set_id not in rank_set_list:
            rank_set_list.append(set_id)
        rank_con_dic.setdefault(set_id,[])
        rank_con_dic[set_id].append(content_id)

    result=[]
    for set in range(0,len(rank_set_list)):
        for con in range(0,len(rank_con_dic[rank_set_list[set]])):
            #print("-- result. set={}, content={}, rank={} "
            #      .format(set, con, rank_con_dic[rank_set_list[set]][con]))
            result.append((set, con, int("{}".format(rank_con_dic[rank_set_list[set]][con]))))
    print("get_order finish")
    return result

def saveContentSimilaritySet(spark, resultSet, ymd, locale, adult_kind):

    print("----- start save contentSimilaritySet ")
    #print(resultSet)
    print("----- ")

    for item in resultSet:
        setId = item[0]
        rank = item[1]
        contentId = item[2]
        #print("setId = {}. contentId={}, rank = {}".format(setId, contentId, rank))
        #print("type = ", type(item) )

    #df = spark.createDataFrame(resultSet, ['set_id', 'rank', 'content_id'])
    df = spark.createDataFrame(resultSet , ["set_id", "content_order", "content_id"])\
        .withColumn("ymd", functions.lit(ymd)).withColumn("locale",functions.lit(locale))\
        .withColumn("adult_kind", functions.lit(adult_kind))\
        .withColumn("created_at", functions.lit(int(round(time.time() * 1000))))
    #df = spark.createDataFrame(resultSet, [0, 1, 2])

    #print("---- colllect : ", df.collect())

    tableName = "actdb.content_similarity_set_{}{}".format(locale[:2], adult_kind)
    df.createOrReplaceTempView("content_similarity_set_tmp")
    spark.sql("drop table if exists " + tableName)
    spark.sql("create table {} as select * from content_similarity_set_tmp".format(tableName))

    print("----- end save content_similarity_set")



    """
    send_list=[]
    locale='ko-KR'
    adult_kind=1
    ymd=20180708
    for i in range(0,len(result)):
        (set_order,content_order,content_id)=result[i]
        send_list.append((locale,adult_kind,int(set_order),int(content_order),int(content_id),ymd))

    conn = pymysql.connect(host='lusso.cmocp2gj1d1i.ap-northeast-2.rds.amazonaws.com', user='lezhin', password='Eoqkrfpwls!',db='actdb', charset='utf8')
    curs = conn.cursor()
    sql="INSERT INTO sim_cls (locale,adult_kind,set_order,content_order,content_id,ymd) values (%s,%s,%s,%s,%s,%s)"
    curs.executemany(sql,send_list)
    conn.commit()
    conn.close()
    """

if __name__ == "__main__":
    print(sys.version)
    print(sys.argv)
    print(sys.argv.__len__())
    if len(sys.argv) < 3:
        print("Error usage: similaritiesCluster [ymd] [locale] [adult_kind] [metastore]")
        sys.exit(-1)
    ymd = sys.argv[1]
    locale = sys.argv[2]
    adult_kind = sys.argv[3]
    metastore = "thrift://insight-v3-m:9083"
    print("ymd: ", ymd)
    print("locale: ", locale)
    print("adult_kind: ", adult_kind)
    if (sys.argv.__len__() > 4):
        metastore = sys.argv[4]
    print("metastore: ", metastore)


    SparkContext.setSystemProperty("hive.metastore.uris", metastore)

    # Spark Session and DataFrame creation
    spark = (SparkSession
             .builder
             .appName('test-hive')
             .config("spark.executor.heartbeatInterval", "36000s")
             .enableHiveSupport()
             .getOrCreate())

    confs = spark.sparkContext.getConf().getAll()
    #get("spark.executor.")
    print("-- confs = {}".format(confs))

    #데이터 로드
    score_dic=dataLoad(spark, locale, adult_kind)

    #데이터프레임 생성
    df_set=pd.DataFrame.from_dict(score_dic)
    df_set=df_set.fillna(0)
    print('----  df_set  --------------')
    #print(df_set)

    #데이터프레임 정규화 및 가중치 조정
    df_set_weight=df_set.copy()
    df_set_weight=get_weight(df_set_weight)

    #클러스터링
    num_cls=int(len(df_set_weight.index)/11)
    # only for test
    #num_cls = 2
    kmeans_result=get_cls(df_set_weight,num_cls)

    #작은 세트 합치기
    con_set_dic=get_blend(df_set, kmeans_result)

    #미디어 데이터 로드
    media_dic=get_media(spark)

    #작품, 세트 오더링
    result=get_order(con_set_dic,media_dic)


    ## temp. send
    saveContentSimilaritySet(spark, result, ymd, locale, adult_kind)


    print("Done!")
    spark.stop
