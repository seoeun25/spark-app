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
from pyspark.sql.types import *

# sudo apt-get install python3-pip
# sudo apt-get install python3-pandas
# sudo apt-get install python3-pymysql
# sudo pip3 install pyhive
# spark-submit wasp-app/src/main/python/similaritiesCluster.py azra brook01.table

def dataLoad(spark, locale="ko-KR"):
    #queryStr = "SELECT user_id, content_id, purchase_cnt FROM actdb.purchase_count_similarity WHERE locale='{}'" \
    #           "".format(locale)

    queryStr = "SELECT source_content_id, target_content_id, abscore " \
               "FROM actdb.content_similarities_spark" \
               " WHERE locale='{}'".format(locale)

    print("   ===  {} ".format(queryStr))
    df_load = spark.sql(queryStr)
    # df_load = spark.sql('select user_id, content_id, locale, adult, purchase_cnt from
    # actdb.purchase_count_similarity limit 10')
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

def get_media():
    conn = presto.connect(host='35.192.91.157')
    curs = conn.cursor()
    sql="""
    SELECT content_id, avg_read_users, sum_read_users
    FROM actdb.read_agg20180706
    """
    curs.execute(sql)
    read_rows = curs.fetchall()
    sql="""
    SELECT content_id, avg_pur_users, avg_pur_cnt, sum_pur_users, sum_pur_cnt
    FROM actdb.pur_agg20180706
    """
    curs.execute(sql)
    pur_rows = curs.fetchall()
    sql="""
    SELECT id, title
    FROM lz_server.media
    """
    curs.execute(sql)
    title_rows = curs.fetchall()
    conn.close()

    media_dic={}

    for i in range(0,len(read_rows)):
        (content_id,avg_read_users,sum_read_users)=read_rows[i]
        media_dic.setdefault(content_id,{})
        media_dic[content_id]['rd_usr']=float(avg_read_users)
        media_dic[content_id]['sum_read_users']=float(sum_read_users)

    for i in range(0,len(pur_rows)):
        (content_id,avg_pur_users,avg_pur_cnt,sum_pur_users,sum_pur_cnt)=pur_rows[i]
        media_dic.setdefault(content_id,{})
        media_dic[content_id]['pur_usr']=float(avg_pur_users)
        media_dic[content_id]['pur_cnt']=float(avg_pur_cnt)
        media_dic[content_id]['sum_pur_user']=float(sum_pur_users)
        media_dic[content_id]['sum_pur_cnt']=float(sum_pur_cnt)

    for i in range(0,len(title_rows)):
        (content_id,title)=title_rows[i]
        media_dic.setdefault(content_id,{})
        media_dic[content_id]['title']=title
    print("media_dic finish")
    return media_dic

def get_order(con_set_dic, media_dic):
    #열람 하나, 구매 하나
    rd_list=[]
    pur_list=[]
    rdsum_list=[]
    pursum_list=[]
    for item in con_set_dic:
        if 'rd_usr' not in media_dic[item]:
            rd_usr=0
        else:
            rd_usr=media_dic[item]['rd_usr']
        rd_list.append((rd_usr,item,con_set_dic[item]))

        if 'pur_cnt' not in media_dic[item]:
            pur_cnt=0
        else:
            pur_cnt=media_dic[item]['pur_cnt']
        pur_list.append((pur_cnt,item,con_set_dic[item]))

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

    rd_list.sort()
    rd_list.reverse()
    pur_list.sort()
    pur_list.reverse()
    rdsum_list.sort()
    rdsum_list.reverse()
    pursum_list.sort()
    pursum_list.reverse()

    media_list=[]
    check_list=[]
    for i in range(0,len(rd_list)):
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
            #print set, rank_set_list[set], con, rank_con_dic[rank_set_list[set]][con]
            result.append((set, con, rank_con_dic[rank_set_list[set]][con]))
    print("get_order finish")
    return result

def sendContentSimilarity(df_set):
    ### 임시코드
    #정규화 유사도 데이터 및 클러스터링 세트 업로드
    #send ab
    send_list=[]
    locale='en-US'
    idx_list=list(df_set.index)
    col_list=list(df_set.columns)
    tmp_list=df_set.values.tolist()
    for idx in range(0,len(tmp_list)):
        for col in range(0,len(tmp_list)):
            send_list.append((locale,int(idx_list[idx]),int(col_list[col]),tmp_list[idx][col]))

    for item in send_list:
        print("send = {}".format(item))
    """
    conn = pymysql.connect(host='lusso.cmocp2gj1d1i.ap-northeast-2.rds.amazonaws.com', user='lezhin', password='Eoqkrfpwls!',db='actdb', charset='utf8')
    curs = conn.cursor()
    sql="INSERT INTO sim_ab (locale,a_id,b_id,ab) values (%s,%s,%s,%s)"
    curs.executemany(sql,send_list)
    conn.commit()
    conn.close()
    """


if __name__ == "__main__":
    print(sys.version)
    print(sys.argv)
    print(sys.argv.__len__())
    if len(sys.argv) != 4:
        print("Error usage: LoadHive [ymd] [locale] [metastore]")
        sys.exit(-1)
    ymd = sys.argv[1]
    locale = sys.argv[2]
    metastore = sys.argv[3]
    print("ymd: ", ymd)
    print("locale: ", locale)
    print("metastore: ", metastore)


    # We have to set the hive metastore uri.
    #hiveMetastore = "thrift://sembp:9083"
    #hiveMetastore = "thrift://insight-v3-m:9083"

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
    score_dic=dataLoad(spark)

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
    media_dic=get_media()

    #작품, 세트 오더링
    result=get_order(con_set_dic,media_dic)


    ## temp. send
    #sendContentSimilarity(df_set)


    print("Done!")
    spark.stop
