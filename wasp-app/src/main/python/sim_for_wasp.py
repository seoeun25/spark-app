import time
import itertools
import math
import copy

import pymysql
from pyhive import presto

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans

def data_load():
    start_time = time.time()
    conn = presto.connect(host='35.192.91.157')
    curs = conn.cursor()
    #KR,US,JP가 순서대로 돌아가도록 수정할 예정
    sql="""
    SELECT user_id, content_id, purchase_cnt
    FROM actdb.sim_agg20180704
    WHERE locale='ko-KR'
    """
    curs.execute(sql)
    rows = curs.fetchall()
    conn.close()

    item_dic={}
    user_dic={}
    for i in range(0,len(rows)):
        (user_id,content_id,cnt)=rows[i]
        item_dic.setdefault(content_id,{})
        item_dic[content_id][user_id]=int(cnt)
        user_dic.setdefault(user_id,{})
        user_dic[user_id][content_id]=int(cnt)

    end_time = time.time()
    print 'data_load', len(rows), int(end_time - start_time), 'sec'
    return (user_dic,item_dic)

#작품별 총점 계산
def data_cal_info(item_dic):
    result={}
    for item in item_dic:
        score=0
        for person in item_dic[item]:
            if item_dic[item][person] >= 10:
                score+=10
            else:
                score+=item_dic[item][person]
        result.setdefault(item, {})
        result[item]=score
    return result

#작품간 스코어 계산
def data_cal_table(user_dic, item_dic):
    start_time = time.time()
    result={}

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
            else: result[other][item]+=user_dic[user][other]
    end_time = time.time()
    print 'data_cal_table', int(end_time - start_time), 'sec'
    return result

def data_cal_sim(table_dic, info_dic):
    result={}
    for item in table_dic:
        a_score = info_dic[item]
        for other in table_dic[item]:
            b_score = info_dic[other]
            ab_score = table_dic[item][other]+table_dic[other][item]
            score = float(ab_score)/(a_score+b_score)
            result.setdefault(item, {})
            result[item][other]=score
    return result

#정규화
def get_scale(df_set):
    for col in df_set.columns:
        df_set[col]=(df_set[col] - df_set[col].min()) / (df_set[col].max() - df_set[col].min())
    for idx in df_set.index:
        df_set.ix[idx]=(df_set.ix[idx] - df_set.ix[idx].min()) / (df_set.ix[idx].max() - df_set.ix[idx].min())
    print 'get_scale'
    return df_set

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

    print 'get_weight'
    return df_set

def get_cls(df_set,num_cls):
    arr_set=np.array(df_set)
    kmeans_result=[]
    kmeans=KMeans(n_clusters=num_cls)
    kmeans.fit(arr_set)
    kmeans_result=list(kmeans.predict(arr_set))
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

            print 'extra', len(set_con_dic), c

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

        print 'merge', len(set_con_dic), c

        #평균값 측정
        all_score=0
        for set in set_con_dic:
            for seed in set_con_dic[set]:
                seed_score=0
                for con in set_con_dic[set]:
                    if seed!=con:
                        seed_score+=df_set.ix[seed][con]
                all_score+=seed_score/(len(set_con_dic[set])-1)
        print i, all_score/len(con_set_dic)
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
    for i in range(0,len(rd_list)):
        (c,rdsum_content_id,rdsum_set_id)=rdsum_list[i]
        if rdsum_content_id not in check_list:
            media_list.append((rdsum_content_id,rdsum_set_id))
            check_list.append(rdsum_content_id)

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
    return result
####

#데이터 로드
(user_dic,item_dic)=data_load()

#인포딕셔너리 생성
info_dic=data_cal_info(item_dic)

#유사도 계산
table_dic=data_cal_table(user_dic, item_dic)
sim_dic=data_cal_sim(table_dic, info_dic)

#데이터프레임 생성
df_set=pd.DataFrame.from_dict(sim_dic)
df_set=df_set.fillna(0)
df_set=get_scale(df_set)

#데이터프레임 정규화 및 가중치 조정
df_set_weight=df_set.copy()
df_set_weight=get_weight(df_set_weight)

#클러스터링
num_cls=int(len(df_set_weight.index)/11)
kmeans_result=get_cls(df_set_weight,num_cls)

#작은 세트 합치기
con_set_dic=get_blend(df_set, kmeans_result)

#미디어 데이터 로드
media_dic=get_media()

#작품, 세트 오더링
result=get_order(con_set_dic,media_dic)




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

conn = pymysql.connect(host='lusso.cmocp2gj1d1i.ap-northeast-2.rds.amazonaws.com', user='lezhin', password='Eoqkrfpwls!',db='actdb', charset='utf8')
curs = conn.cursor()
sql="INSERT INTO sim_ab (locale,a_id,b_id,ab) values (%s,%s,%s,%s)"
curs.executemany(sql,send_list)
conn.commit()
conn.close()


#send cls
send_list=[]
locale='ko-KR'
adult=1
ymd=20180708
for i in range(0,len(result)):
    (set_order,content_order,content_id)=result[i]
    send_list.append((locale,adult,int(set_order),int(content_order),int(content_id),ymd))

conn = pymysql.connect(host='lusso.cmocp2gj1d1i.ap-northeast-2.rds.amazonaws.com', user='lezhin', password='Eoqkrfpwls!',db='actdb', charset='utf8')
curs = conn.cursor()
sql="INSERT INTO sim_cls (locale,adult,set_order,content_order,content_id,ymd) values (%s,%s,%s,%s,%s,%s)"
curs.executemany(sql,send_list)
conn.commit()
conn.close()