# coding: utf-8
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql import SparkSession
import time
import numpy as np
from scipy import stats
from datetime import datetime,timedelta
from datetime import date
from requests.auth import HTTPBasicAuth
import requests
import json


cluster_dict = {
    'vlog':             ('sg_kafka_likee_topic_message','new_kafka_sg_like_node_exporter','sg-pulsar-likee','like_user_event')
    ,'likee_prod':      ('sg_kafka_likee_topic_message','new_kafka_sg_like_node_exporter','sg_pulsar_likee')
    ,'likee_wall_paper':('sg_kafka_likee_topic_message','new_kafka_sg_like_node_exporter','sg_pulsar_likee','likee_wall_paper_show_user_event_105')
    ,'bigolive':  ('sg_kafka_live_topic_message','new_kafka_sg_live_node_exporter', 'sg-pulsar-live', 'bigo_show_user_event')
    ,'livelite':  ('sg_kafka_live_topic_message','new_kafka_sg_live_node_exporter', 'sg_pulsar_live','bigo_show_user_event_601')
    ,'bigochat':  ('sg_kafka_live_topic_message','new_kafka_sg_live_node_exporter', 'sg_pulsar_live','bigochat_show_user_event_602')
    ,'vpn':       ('sg_kafka_live_topic_message','new_kafka_sg_live_node_exporter', 'sg_kafka_live','vpn_show_user_event_101')
    ,'pinpincart':('sg_kafka_live_topic_message','new_kafka_sg_live_node_exporter', 'sg_kafka_live')
    ,'hello':   ('kafka_gz_hello_topic_messages','new_kafka_gz_hello_node_exporter', '')
    ,'fire':    ('kafka_gz_hello_topic_messages','new_kafka_gz_hello_node_exporter','')
    ,'common':      ('sg_kafka_common_topic_message','new_kafka_sg_common_node_exporter', 'sg-pulsar-common')
    ,'ludo':        ('sg_kafka_common_topic_message','new_kafka_sg_common_node_exporter', 'sg-pulsar-common','bigo_show_user_event_52')
    ,'hellotalk':   ('sg_kafka_common_topic_message','new_kafka_sg_common_node_exporter', 'sg-pulsar-common','bigo_show_user_event_66')
    ,'indigo':      ('sg_kafka_indigo_topic_message','new_kafka_sg_indigo_node_exporter', 'sg-pulsar-indigo','indigo_show_user_event_62')
    ,'cupid':       ('sg_kafka_common_topic_message','new_kafka_sg_common_node_exporter', 'sg-pulsar-common')
    ,'woodpecker':  ('sg_kafka_common_topic_message','new_kafka_sg_common_node_exporter', 'sg-pulsar-common')
    ,'voice_club':  ('sg_kafka_common_topic_message','new_kafka_sg_common_node_exporter', 'sg-pulsar-common','voice_club_show_user_event_98')
    ,'liggs_bigwin':('sg_kafka_common_topic_message','new_kafka_sg_common_node_exporter', 'sg-pulsar-common','liggs_bigwin_show_user_event_112')
    ,'bigoface':    ('sg_kafka_common_topic_message','new_kafka_sg_common_node_exporter', 'sg-pulsar-common','bigo_show_user_event_1001')
    ,'bigopay':     ('sg_kafka_common_topic_message','new_kafka_sg_common_node_exporter', 'sg-pulsar-common','bigo_show_user_event_1004')
    ,'bigo_ad':     ('sg08_kafka_ad_topic_message', 'new_kafka_sg08_ad_node_exporter', 'sg08-pulsar-ad')
    ,'tiki':        ('sg_kafka_tiki_topic_message', 'new_kafka_sg_tiki_node_exporter', 'sg-pulsar-tiki','bigo_show_user_event_95')
    ,'tob':         ('sg10_kafka_ad_topic_message', 'new_kafka_sg10_ad_node_exporter', 'sg10-pulsar-ad')
    ,'algo_cv':     ('sg_kafka_likee_topic_message', 'new_kafka_sg_like_node_exporter','')
    ,'algo':        ('sg_kafka_live_topic_message', 'new_kafka_sg_live_node_exporter','sg_kafka_live')
}


def wetchat_robot(message, keys, msgtype, user):
    url = 'http://proxy.bigdata.bigo.inner/wechat_bot/cgi-bin/webhook/send?key=%s' % keys
    headers = {'content-type': 'application/json;charset=UTF-8'}
    requestData = {
        "msgtype": "%s" % msgtype,
        "%s" % msgtype: {
            "content": message,
            "mentioned_list": ["%s" % user]
            }
    }
    ret = requests.post(url, json=requestData, headers=headers)
    # 控制发送频率
    time.sleep(1)
    if ret.status_code == 200:
        text = json.loads(ret.text)
        print(text)

def parse_ymd(s):
    year_s, mon_s, day_s = s.split('-')
    return datetime(int(year_s), int(mon_s), int(day_s))


# 正态分布校验
def std_check(to_compare, k=2.5):
    # 计算期望及方差
    if len(to_compare) > 1:
        mean = np.mean(to_compare)
        std = np.std(to_compare, ddof=1)
        # 异常级别
        too_big = mean + std*k
        too_small = mean - std*k
        too_big = 0 if np.isnan(too_big) or too_big<=0 else too_big
        too_small = 0 if np.isnan(too_small) or too_small<=0 else too_small
        # print 'too_big:%s, too_small:%s' % (too_big, too_small)
        return long(too_big), long(too_small)
    elif len(to_compare) == 1:
        return to_compare[0] * 1.7, to_compare[0] * 0.3
    else:
        return 0, 0


def linear_regression_check(to_compare, k=0.7):
    if len(to_compare) > 1:
        slope, intercept, r_value, p_value, std_err = stats.linregress([i for i in range(len(to_compare))], to_compare)
        # 计算期线性回归期望值
        mean = len(to_compare) * slope + intercept
        too_big = mean * (1.3 + k)
        too_small = mean * (1 - k)
        too_big = 0 if np.isnan(too_big) or too_big<=0 else too_big
        too_small = 0 if np.isnan(too_small) or too_small<=0 else too_small
        return long(too_big), long(too_small)
    elif len(to_compare) == 1:
        return to_compare[0] * (1.3 + k), to_compare[0] * (1 - k)
    else:
        return 0, 0


def get_grafana_url(input, hour):
    res_dict = {'tb_name': set(), 'tb_source': set()}
    base_url = "http://metrics-sysop.bigo.sg/d/H3baMTH4z/biao-kong-jian-da-xiao-bo-dong-yi-chang-jian-kong?orgId=1&from=now-7d&to=now"
    if len(input) < 10:
        for i in input:
            tb_name, tb_source = i
            if tb_source not in res_dict['tb_source']:
                res_dict['tb_source'].add(tb_source)
            if tb_source not in res_dict['tb_name']:
                res_dict['tb_name'].add(tb_name)
        for tb_source in res_dict['tb_source']:
            base_url += '&var-tb_source=%s' % tb_source
        if hour == '--':
            for tb_name in res_dict['tb_name']:
                base_url += '&var-tb_name=%s(day)' % tb_name.replace('+', '%2B')
            base_url += '&var-hour=00'
        else:
            for tb_name in res_dict['tb_name']:
                base_url += '&var-tb_name=%s(hour)' % tb_name.replace('+', '%2B')
            base_url += '&var-hour=%s' % hour
    return base_url


def get_normal_event_url(tb_name, topic_name):
    kafka_url = "http://metrics-sysop.bigo.sg/d/ksjmVstGk-new/kafka_topic_monitor_all-sg-new?viewPanel=206&orgId=1&from=now-7d&to=now"
    pulsar_url = "http://metrics-sysop.bigo.sg/d/3xEtii5mk/pulsar-topic?viewPanel=67&orgId=1&from=now-2d&to=now"
    db, table = tb_name.split('.')
    if db in cluster_dict:
        jobname, cluster, pulsar_cluster = cluster_dict[db][0], cluster_dict[db][1],cluster_dict[db][2]
    else:
        print '%s not found in cluster' % tb_name
        return 'unknown', 'unknown'
    app_name = db if db != 'vlog' else 'like'
    if db == 'bigo_ad':
        pulsar_url += '&var-job=sg10-pulsar-ad&var-outtopic=persistent:%%2F%%2Fbigo_ad%%2Fbaina%%2F%s' % topic_name
    else:
        pulsar_url += '&var-job=%s&var-outtopic=persistent:%%2F%%2F%s%%2Fbaina%%2F%s' % (pulsar_cluster, app_name, topic_name)
    kafka_url += '&var-topic_jobname=%s&var-cluster=%s&var-topic=%s' % (jobname, cluster, topic_name)
    return kafka_url, pulsar_url


def get_bdp_url(topic_name):
    kafka_url = "http://metrics-sysop.bigo.sg/d/ksjmVstGk-new/kafka_topic_monitor_all-sg-new?viewPanel=206&orgId=1&from=now-7d&to=now"
    kafka_url += '&var-topic_jobname=kafka_sg_bdp_topic_messages&var-cluster=new_kafka_sg_bdp_exporter&var-topic=%s' % topic_name
    return kafka_url


def get_common_event_url(tb_name):
    pulsar_url = "http://metrics-sysop.bigo.sg/d/3xEtii5mk/pulsar-topic?viewPanel=67&orgId=1&from=now-2d&to=now"
    brpc_url = "http://metrics-sysop.bigo.sg/d/Vpg_vSxGz/baina-topic-statistics-sg?orgId=1&from=now-2d&to=now"
    im_url = "http://metrics-sysop.bigo.sg/d/izpczyQMz/baina-woodpecker-topic-statistics-sg?orgId=1&from=now-2d&to=now"
    db = tb_name.split('.')[0]
    event_id = tb_name.split('+')[1]
    if db in cluster_dict:
        pulsar_cluster, mother_topic = cluster_dict[db][2], cluster_dict[db][3]
    else:
        print '%s not found in cluster' % tb_name
        return 'unknown', 'unknown', 'unknown'
    app_name = db if db != 'vlog' else 'like'
    pulsar_url += '&var-job=%s&var-outtopic=persistent:%%2F%%2F%s%%2Fbaina%%2F%s_%s' %(pulsar_cluster, app_name, mother_topic, event_id)
    brpc_url += '&var-topic=%s_%s' % (mother_topic, event_id)
    im_url += '&var-topic=%s_%s' % (mother_topic, event_id)
    return pulsar_url,brpc_url,im_url



if __name__ == '__main__':
    day = sys.argv[1]
    hour = sys.argv[2]
    if '--' != hour:
        # 非全量表空间大小低于1M的告警需要忽略，需要根据经验进行调整
        min_size = 1024 *1024
        sql_str = """
                select
                t1.tb_name
                ,t1.tb_source
                ,t1.event_id
                ,t1.hdfs_path
                ,t1.abnormal_info
                ,t1.file_size
                ,t1.day
                ,unix_timestamp(concat(t1.day,' ',t1.hour,':00:00')) as update_time
                ,coalesce(t1.table_size,0) as table_size
                ,coalesce(t1.query_times,0) as query_times
                from dwd.count_hdfs_size_hi t1
                left join dwd.hdfs_size_monitor_whitelist_di t2 on if(t1.tb_source!='common_event', t1.tb_name, concat(t1.tb_name,'+',t1.event_id))=t2.tb_name
                left join 
                (
                    select 
                    tb_name
                    ,sum(flume_flag+datay_flag) as flag_num
                    from
                    (
                        select tb_name
                        ,if(hdfs_path REGEXP 'flume',1,0) as flume_flag
                        ,if(hdfs_path REGEXP 'datay/baina_yy',1,0) as datay_flag
                        from dwd.count_hdfs_size_hi 
                        where day between date_sub('%s', 10) and '%s' and hour='%s' and tb_source='normal_event'
                        group by tb_name
                        ,if(hdfs_path REGEXP 'flume',1,0)
                        ,if(hdfs_path REGEXP 'datay/baina_yy',1,0) 
                    ) t
                    group by tb_name
                    having sum(flume_flag+datay_flag)>=2
                ) t3 on t1.tb_name=t3.tb_name
                where day between date_sub('%s', 10) and '%s' and hour='%s' and t1.abnormal_info='normal'
                and t1.tb_name not regexp 'tiki'
                and t2.tb_name is null and ((t3.tb_name is not null and t1.hdfs_path not REGEXP 'flume')  or t3.tb_name is null)
                order by t1.day
                """ % (day, day, hour,day, day, hour)

    else:
        # 全量表空间大小低于10M的告警需要忽略，需要根据经验进行调整
        min_size = 1024*1024*10
        sql_str = """
                select
                t1.tb_name
                ,t1.tb_source
                ,t1.event_id
                ,t1.hdfs_path
                ,t1.abnormal_info
                ,t1.file_size
                ,t1.day
                ,unix_timestamp(concat(t1.day,' 00:00:00')) as update_time
                ,coalesce(t1.table_size,0) as table_size
                ,coalesce(t1.query_times,0) as query_times
                from dwd.count_hdfs_size_di t1
                left join dwd.hdfs_size_monitor_whitelist_di t2 on if(t1.tb_source!='common_event', t1.tb_name, concat(t1.tb_name,'+',t1.event_id))=t2.tb_name
                left join 
                (
                    select 
                    tb_name
                    ,sum(flume_flag+datay_flag) as flag_num
                    from
                    (
                        select tb_name
                        ,if(hdfs_path REGEXP 'flume',1,0) as flume_flag
                        ,if(hdfs_path REGEXP 'datay/baina_yy',1,0) as datay_flag
                        from dwd.count_hdfs_size_di 
                        where day between date_sub('%s', 10) and '%s' and tb_source='normal_event'
                        group by tb_name
                        ,if(hdfs_path REGEXP 'flume',1,0)
                        ,if(hdfs_path REGEXP 'datay/baina_yy',1,0) 
                    ) t
                    group by tb_name
                    having sum(flume_flag+datay_flag)>=2
                ) t3 on t1.tb_name=t3.tb_name
                where day between date_sub('%s', 10) and '%s' and t1.abnormal_info='normal'
                and t1.tb_name not regexp 'tiki'
                and t2.tb_name is null and ((t3.tb_name is not null and t1.hdfs_path not REGEXP 'flume')  or t3.tb_name is null)
                order by t1.day
                """ % (day, day, day, day)
    sparkSession = SparkSession.builder.appName('get_hdfs_size').enableHiveSupport().getOrCreate()
    hive_ret = sparkSession.sql(sql_str)
    ret = hive_ret.collect()
    current, to_compare = {}, {}
    for r in ret:
        if r.day == day:
            if (r.tb_name, r.event_id, r.tb_source) not in current:
                current[(r.tb_name, r.event_id, r.tb_source)] = [long(r.file_size), r.update_time, r.table_size, r.query_times]
        else:
            if (r.tb_name, r.event_id, r.tb_source) not in to_compare:
                to_compare[(r.tb_name, r.event_id, r.tb_source)] = []
            # 对于imo_us集群的数据，超过3天的数据会被压缩，这里的限制是为了排除压缩数据对样本的影响，只收集有效数据
            if not (r.tb_source == 'imo_us' and (datetime.now() - parse_ymd(r.day)).days > 3 and r.file_size == 0):
                to_compare[(r.tb_name, r.event_id, r.tb_source)].append([long(r.file_size), r.update_time, r.table_size, r.query_times])

    err_info = []
    for k in sorted(current.keys()):
        if k in to_compare:
            # 正态分布区间
            too_big, too_small = std_check([c[0] for c in to_compare[k]])
            # 线性区间
            too_big2, too_small2 = linear_regression_check([c[0] for c in to_compare[k]])
            # 取两者的并集区间，不满足时才触发告警
            too_big = too_big2 if too_big2 > too_big else too_big
            too_small = too_small2 if too_small2 < too_small else too_small
            # too_small不能为负数
            too_small = too_small if too_small > 0 else 0
            mean = (too_small+too_big)/2
            # 当前值大于min_size且在置信区间外，或者，当前值为0，too_small大于0时，触发告警
            if (current[k][0] < too_small or current[k][0] > too_big) or (current[k][0] == 0 and mean > min_size):
                tb_full_name = k[0] if k[2] != 'common_event' else '%s+%s' % (k[0], k[1])
                err_info.append([tb_full_name, k[2], current[k][0], current[k][1], current[k][2],current[k][3]])
                for compare in to_compare[k]:
                    # 更新to_compare里的table_size，query_times
                    err_info.append([tb_full_name, k[2], compare[0], compare[1], current[k][2],current[k][3]])
    # 当日数据缺少的维度也需要告警
    for k in sorted(to_compare.keys()):
        if k not in current:
            too_big2, too_small2 = linear_regression_check([c[0] for c in to_compare[k]])
            mean = (too_big2 + too_small2)/2
            # 该维度均值大于min_size才进行告警
            if mean > min_size:
                tb_full_name = k[0] if k[2] != 'common_event' else '%s+%s' % (k[0], k[1])
                update_time = time.mktime(time.strptime(day, '%Y-%m-%d')) if hour == '--' else time.mktime(time.strptime('%s %s:00:00' % (day, hour), '%Y-%m-%d %H:%M:%S'))
                err_info.append([tb_full_name, k[2], 0, update_time, to_compare[k][0][2], 0])
                for compare in to_compare[k]:
                    err_info.append([tb_full_name, k[2], compare[0], compare[1], compare[2], compare[3]])
    if len(err_info) > 0:
        sql_abnormal_str = ''
        if hour == '--':
            insert_tb = 'dwd.get_abnormal_table_di'
            for err in err_info:
                # tb_name,tb_source,file_size,update_time,table_size,query_times,day
                sql_abnormal_str += "('%s','%s',%s,%s,%s,%s,'','%s')," % (err[0], err[1], err[2],err[3],err[4],err[5], day)
        else:
            insert_tb = 'dwd.get_abnormal_table_hi'
            for err in err_info:
                # tb_name,tb_source,file_size,update_time,table_size,query_times,day,hour
                sql_abnormal_str += "('%s','%s',%s,%s,%s,%s,'','%s','%s')," % (err[0], err[1], err[2],err[3],err[4],err[5], day, hour)
        sql_abnormal_str = sql_abnormal_str[:-1]
        sql_insert = "insert overwrite table %s values %s;" % (insert_tb, sql_abnormal_str)
        print sql_insert
        sparkSession2 = SparkSession.builder.appName('%s-%s-%s' % (insert_tb, day, hour)).enableHiveSupport().getOrCreate()
        hive_ret2 = sparkSession2.sql(sql_insert)
    grafana_url_hour = "http://metrics-sysop.bigo.sg/d/H3baMTH4z/biao-kong-jian-da-xiao-yi-chang-jian-kong?orgId=1&from=now-7d&to=now"
    grafana_url_day = "http://metrics-sysop.bigo.sg/d/37mIlTv4z/zuo-ri-yi-chang-biao?orgId=1&from=now-2d&to=now"
    grafana_info = list()
    # 按天产出断流信息表即可
    if hour == '--':
        # 每天发送一次汇总信息到企业微信群
        sql_summary = """
                    select 
                    t1.*
                    ,t2.table_size
                    ,t2.query_times
                    ,if(t1.tb_source='imo_us', concat('/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/',t1.tb_name),t2.hdfs_path) as hdfs_path
                    ,coalesce(t3.topic_name, '') as topic_name
                    from
                    (
                        select
                        1 as flag
                        ,tb_source
                        ,tb_name
                        ,abnormal_info
                        ,day
                        from dwd.cut_off_table_di
                        where day between date_sub('%s',1) and '%s' and ((tb_source ='imo_us' and query_times>0) 
                          or (tb_source!='imo_us' and query_times=0 and table_size>=10737418240)
                          or (query_times>=1 and query_times<=3 and table_size>=104857600)
                          or query_times>3
                          or tb_source='common_event'
                          )
                        union all 
                        select
                        2 as flag
                        ,tb_source
                        ,tb_name
                        ,max(if(update_time=unix_timestamp('%s 00:00:00'),file_size,0)) as abnormal_info
                        ,max(day) as day
                        from dwd.get_abnormal_table_di
                        where day='%s'
                        and tb_name not in (select tb_name from dwd.hdfs_size_monitor_whitelist_di) 
                        and tb_name not in (select tb_name from dwd.cut_off_table_di where day='%s')
                        group by tb_source,tb_name
                    ) t1 
                    join 
                    (
                        select
                        tb_source
                        ,if(tb_source='common_event', concat(tb_name,'+',event_id), tb_name) as tb_name
                        ,hdfs_path
                        ,table_size
                        ,query_times
                        from dwd.get_table_hdfs_path_di
                        where (tb_source ='imo_us' and query_times>0) 
                          or (tb_source!='imo_us' and query_times=0 and table_size>=10737418240)
                          or (query_times>=1 and query_times<=3 and table_size>=104857600)
                          or query_times>3
                          or tb_source='common_event'
                    ) t2 on t1.tb_source=t2.tb_source and t1.tb_name=t2.tb_name
                    left join 
                    (
                        select
                        trim(lower(concat(hive_db,'.',hive_table))) as tb_name
                        ,replace(topic_name,' ', '') as topic_name
                        from common.sg_owl_service_adm_oss_hive_exp_tbl_task_orc
                        where task_type=3 and dest_cluster_id=1 and status=3 and area='sg' 
                        and hive_db!='tmp' 
                        group by 
                        trim(lower(concat(hive_db,'.',hive_table)))
                        ,replace(topic_name,' ', '')
                        union all
                        SELECT
                        trim(lower(hive_table)) as tb_name
                        ,replace(final_topic,' ', '') as topic_name
                        from common.sg_owl_service_adm_oss_cfg_event_orc
                        where is_common = 0 and hive_table!=''
                        group by 
                        trim(lower(hive_table))
                        ,replace(final_topic,' ', '')
                    ) t3 on t1.tb_name=t3.tb_name                          
                """ % (day, day, day, day, day)
        sparkSession4 = SparkSession.builder.appName('cut_off_table_di_summary').enableHiveSupport().getOrCreate()
        print 'sql_summary:', sql_summary
        hive_ret4 = sparkSession4.sql(sql_summary).collect()
        today_dic, yesterday_dict, bf_yesterday_dict, diff_dict = {}, {}, {}, {'today': [], 'yesterday': [], '3day_continue': []}
        Total, EmptyTable, TaskStop, CutOff, PartitionNotExists, MonthPartition = 0, 0 ,0, 0, 0, 0
        for r in hive_ret4:
            if r.day == day and int(r.flag) == 1:
                if r.tb_name not in today_dic:
                    today_dic[r.tb_name] = [r.abnormal_info, r.tb_source, r.table_size, r.query_times]
                if 'CutOff' in r.abnormal_info:
                    CutOff += 1
                elif 'TaskStop' in r.abnormal_info:
                    TaskStop += 1
                elif 'EmptyTable' == r.abnormal_info:
                    EmptyTable += 1
                elif 'PartitionNotExists' == r.abnormal_info:
                    PartitionNotExists += 1
                elif 'MonthPartition' == r.abnormal_info:
                    MonthPartition += 1
                else:
                    raise Exception(r.abnormal_info)
                Total += 1
            elif r.day == (parse_ymd(day) - timedelta(1)).strftime('%Y-%m-%d') and int(r.flag) == 1:
                if r.tb_name not in yesterday_dict:
                    yesterday_dict[r.tb_name] = [r.abnormal_info, r.tb_source, r.table_size, r.query_times]
            elif int(r.flag) == 2:
                if r.tb_name not in bf_yesterday_dict:
                    diff_dict['3day_continue'].append((r.tb_name,r.hdfs_path, r.tb_source, r.table_size, r.query_times, r.abnormal_info, r.topic_name))
        for k, v in today_dic.items():
            if k not in yesterday_dict:
                diff_dict['today'].append((k, v))
        for k, v in yesterday_dict.items():
            if k not in today_dic:
                diff_dict['yesterday'].append((k, v))
        msg = "# <font color=\"info\">表hdfs异常信息汇总</font>-<font color=\"warning\">天级(%s):</font>\n\n" % day
        msg += "Total:%i\nEmptyTable:%i\nTaskStop(调度已停止):%i\nCutOff(数据已断流):%i\nPartitionNotExists(打点事件分区缺失):%i\nMonthPartition(月分区表可忽略):%i\n" % (Total, EmptyTable, TaskStop, CutOff, PartitionNotExists,MonthPartition)
        index = 1
        if len(diff_dict['today']) > 0:
            msg += "今日新增异常表:\n"
            for t in sorted(diff_dict['today'], key=lambda x:(x[1][3], x[1][2]), reverse=True):
                if long(t[1][2]) >= 1024**4:
                    table_size = '%0.2fTb' % (long(t[1][2])*1.0 / 1024**4)
                elif long(t[1][2]) >= 1024**3:
                    table_size = '%0.2fGb' % (long(t[1][2])*1.0 / 1024**3)
                elif long(t[1][2]) >= 1024**2:
                    table_size = '%0.2fMb' % (long(t[1][2])*1.0 / 1024**2)
                else:
                    table_size = '%0.2fKb' % (long(t[1][2])*1.0 / 1024)
                msg += "%i.%s(来源:%s-表大小:<font color=\"warning\">%s</font>-查询次数:<font color=\"warning\">%s</font>):%s\n" % (index, t[0], t[1][1],table_size,t[1][3], t[1][0])
                index += 1
                if len(msg.encode('utf-8')) > 3000:
                    print msg
                    wetchat_robot(msg, '30afa32f-a0d7-4960-91bf-143e914b1719', 'markdown', '')
                    msg = ''
        if len(diff_dict['yesterday']) > 0:
            msg += "今日消失异常表:\n"
            for t in sorted(diff_dict['yesterday'], key=lambda x:(x[1][3], x[1][2]), reverse=True):
                if long(t[1][2]) >= 1024**4:
                    table_size = '%0.2fTb' % (long(t[1][2])*1.0 / 1024**4)
                elif long(t[1][2]) >= 1024**3:
                    table_size = '%0.2fGb' % (long(t[1][2])*1.0 / 1024**3)
                elif long(t[1][2]) >= 1024**2:
                    table_size = '%0.2fMb' % (long(t[1][2])*1.0 / 1024**2)
                else:
                    table_size = '%0.2fKb' % (long(t[1][2])*1.0 / 1024)
                msg += "%i.%s(来源:%s-表大小:<font color=\"warning\">%s</font>-查询次数:<font color=\"warning\">%s</font>):%s\n" % (index, t[0], t[1][1], table_size, t[1][3], t[1][0])
                index += 1
                if len(msg.encode('utf-8')) > 3000:
                    print msg
                    wetchat_robot(msg, '30afa32f-a0d7-4960-91bf-143e914b1719', 'markdown', '')
                    msg = ''
        if len(diff_dict['3day_continue']) > 0:
            msg += "昨日数据波动异常的表:\n"
            for t in sorted(diff_dict['3day_continue'], key=lambda x:(x[4],x[3]), reverse=True):
                tb_name, hdfs_path, tb_source, table_size, query_times, file_size, topic_name = t
                if long(file_size) >= 1024 * 1024 * 1024:
                    file_size_str = '%sGb' % round(float(file_size) / (1024 ** 3), 2)
                elif long(file_size) >= 1024 * 1024:
                    file_size_str = '%sMb' % round(float(file_size) / (1024 ** 2), 2)
                else:
                    file_size_str = '%skb' % round(float(file_size) / 1024, 2)
                if long(table_size) >= 1024**4:
                    table_size_str = '%0.2fTb' % (long(table_size)*1.0 / 1024**4)
                elif long(table_size) >= 1024**3:
                    table_size_str = '%0.2fGb' % (long(table_size)*1.0 / 1024**3)
                elif long(table_size) >= 1024**2:
                    table_size_str = '%0.2fMb' % (long(table_size)*1.0 / 1024**2)
                else:
                    table_size_str = '%0.2fKb' % (long(table_size)*1.0 / 1024)
                if tb_source == 'normal_event':
                    kafka_url, pulsar_url = get_normal_event_url(tb_name, topic_name)
                    msg += "%i.%s(来源:%s-表大小:<font color=\"warning\">%s</font>-查询次数:<font color=\"warning\">%s</font>)\n路径:%s\n昨日:%s|[kafka](%s)|[pulsar](%s)\n" \
                           % (index, tb_name, tb_source, table_size_str, query_times, hdfs_path, file_size_str, kafka_url, pulsar_url)
                elif tb_source == 'common_event':
                    pulsar_url, brpc_url, im_url = get_common_event_url(tb_name)
                    msg += "%i.%s(来源:%s-表大小:<font color=\"warning\">%s</font>-查询次数:<font color=\"warning\">%s</font>)\n路径:%s\n昨日:%s|[pulsar](%s)|[brpc](%s)|[im](%s)\n" \
                           % (index, tb_name, tb_source, table_size_str, query_times, hdfs_path, file_size_str, pulsar_url, brpc_url, im_url)
                elif tb_source == 'bdp':
                    kafka_url = get_bdp_url(topic_name)
                    msg += "%i.%s(来源:%s-表大小:<font color=\"warning\">%s</font>-查询次数:<font color=\"warning\">%s</font>)\n路径:%s\n昨日:%s|[kafka](%s)\n" \
                           % (index, tb_name, tb_source, table_size_str, query_times, hdfs_path, file_size_str, kafka_url)
                else:
                    msg += "%i.%s(来源:%s-表大小:<font color=\"warning\">%s</font>-查询次数:<font color=\"warning\">%s</font>)\n路径:%s\n昨日:%s\n" \
                           % (index, tb_name, tb_source, table_size_str, query_times, hdfs_path, file_size_str)
                index += 1
                grafana_info.append((tb_name, tb_source))
                if len(msg.encode('utf-8')) > 3000:
                    print msg
                    wetchat_robot(msg, '30afa32f-a0d7-4960-91bf-143e914b1719', 'markdown', '')
                    msg = ''
        if index == 1:
            msg += "昨日数据完全正常\n"
        msg += "## 数据波动汇总见[grafana](%s)" % get_grafana_url(grafana_info, hour)
        print 'msg长度为:%i\n' % len(msg.encode('utf-8')), msg
        wetchat_robot(msg, '30afa32f-a0d7-4960-91bf-143e914b1719', 'markdown', '')
    # 环比数据中的异常，需要往前排查2个小时临近数据，如果也异常，说明连续三个小时异常，需要企业微信发出告警进行人工确认
    else:
        st = (parse_ymd(day) + timedelta(hours=int(hour) - 2)).strftime('%Y-%m-%d %H:%M:%S')
        et = (parse_ymd(day) + timedelta(hours=int(hour))).strftime('%Y-%m-%d %H:%M:%S')
        if hour in ('00', '01'):
            where_condition = "day between date_sub('%s', 1) and '%s' and from_unixtime(update_time) between '%s' and '%s'" % (day, day, st, et)
        else:
            where_condition = "day='%s' and from_unixtime(update_time) between '%s' and '%s'" % (day, st, et)
        sql_compare = """
                        select 
                        t1.tb_source
                        ,t1.tb_name
                        ,t1.file_size
                        ,t2.table_size
                        ,t2.query_times
                        ,if(t1.tb_source='imo_us', concat('/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/',t1.tb_name),t2.hdfs_path) as hdfs_path
                        ,coalesce(t4.topic_name, '') as topic_name
                        from
                        (
                            select
                            tb_source
                            ,tb_name
                            ,max(if(hour='%s',file_size,0)) as file_size
                            from dwd.get_abnormal_table_hi
                            where %s
                            and tb_name not in (select tb_name from dwd.hdfs_size_monitor_whitelist_di)
                            and tb_name not in (select tb_name from dwd.cut_off_table_di where day BETWEEN date_sub('%s',2) and date_sub('%s',1) group by tb_name)
                            group by tb_source,tb_name
                            having count(DISTINCT hour)=3
                        ) t1
                        join 
                        (
                            select
                            tb_source
                            ,if(tb_source='common_event', concat(tb_name,'+',event_id), tb_name) as tb_name
                            ,hdfs_path
                            ,table_size
                            ,query_times
                            from dwd.get_table_hdfs_path_di
                            where (tb_source ='imo_us' and query_times>0) 
                              or (tb_source!='imo_us' and query_times=0 and table_size>=10737418240)
                              or (query_times>=1 and query_times<=3 and table_size>=104857600)
                              or query_times>3
                              or tb_source='common_event'
                        ) t2 on t1.tb_source=t2.tb_source and t1.tb_name=t2.tb_name
                        left join
                        (
                            select
                            trim(lower(concat(hive_db,'.',hive_table))) as tb_name
                            ,topic_name
                            from common.sg_owl_service_adm_oss_hive_exp_tbl_task_orc
                            where task_type=3 and dest_cluster_id=1 and status=3 and area='sg' 
                            and hive_db!='tmp' 
                            union all
                            SELECT
                            trim(lower(hive_table)) as tb_name
                            ,final_topic as topic_name
                            from common.sg_owl_service_adm_oss_cfg_event_orc
                            where is_common = 0 and hive_table!=''
                        ) t4 on t1.tb_name=t4.tb_name
                        """ % (hour,where_condition, day, day)
        sparkSession4 = SparkSession.builder.appName('cut_off_table_hi_summary').enableHiveSupport().getOrCreate()
        hive_ret4 = sparkSession4.sql(sql_compare).collect()
        hour_dic = list()
        msg = "# <font color=\"info\">表hdfs大小异常波动</font>-<font color=\"warning\">-小时级(%s点):</font>\n\n" % hour
        msg += "以下表连续三个小时出现异常，请人工确认:\n"
        index = 1
        for r in sorted(hive_ret4, key=lambda x: (x.query_times,x.table_size), reverse=True):
            if long(r.file_size)>=1024*1024*1024:
                file_size = '%sGb' % round(float(r.file_size)/(1024**3),2)
            elif long(r.file_size)>=1024*1024:
                file_size = '%sMb' % round(float(r.file_size)/(1024**2),2)
            else:
                file_size = '%skb' % round(float(r.file_size)/1024, 2)
            if long(r.table_size) >= 1024 ** 4:
                table_size = '%0.2fTb' % (long(r.table_size) * 1.0 / 1024 ** 4)
            elif long(r.table_size) >= 1024 ** 3:
                table_size = '%0.2fGb' % (long(r.table_size) * 1.0 / 1024 ** 3)
            elif long(r.table_size) >= 1024 ** 2:
                table_size = '%0.2fMb' % (long(r.table_size) * 1.0 / 1024 ** 2)
            else:
                table_size = '%0.2fKb' % (long(r.table_size) * 1.0 / 1024)
            if r.tb_source == 'normal_event':
                kafka_url, pulsar_url = get_normal_event_url(r.tb_name, r.topic_name)
                msg += '%i.%s(来源:%s-表大小:<font color=\"warning\">%s</font>-查询次数:<font color=\"warning\">%s</font>)\n路径:%s\n当前小时:%s|[kafka](%s)|[pulsar](%s)\n' % (
                index, r.tb_name, r.tb_source, table_size, r.query_times, r.hdfs_path, file_size, kafka_url, pulsar_url)
            elif r.tb_source == 'common_event':
                pulsar_url, brpc_url, im_url = get_common_event_url(r.tb_name)
                msg += '%i.%s(来源:%s-表大小:<font color=\"warning\">%s</font>-查询次数:<font color=\"warning\">%s</font>)\n路径:%s\n当前小时:%s|[pulsar](%s)|[brpc](%s)|[im](%s)\n' % (
                index, r.tb_name, r.tb_source, table_size, r.query_times, r.hdfs_path, file_size, pulsar_url, brpc_url, im_url)
            elif r.tb_source == 'bdp':
                kafka_url = get_bdp_url(r.topic_name)
                msg += '%i.%s(来源:%s-表大小:<font color=\"warning\">%s</font>-查询次数:<font color=\"warning\">%s</font>)\n路径:%s\n当前小时:%s|[kafka](%s)\n' % (
                index, r.tb_name, r.tb_source, table_size, r.query_times, r.hdfs_path, file_size,kafka_url)
            else:
                msg += '%i.%s(来源:%s-表大小:<font color=\"warning\">%s</font>-查询次数:<font color=\"warning\">%s</font>)\n路径:%s\n当前小时:%s\n' % (
                index, r.tb_name, r.tb_source, table_size, r.query_times, r.hdfs_path, file_size)
            index += 1
            grafana_info.append((r.tb_name, r.tb_source))
            if len(msg.encode('utf-8')) > 3000:
                wetchat_robot(msg, '30afa32f-a0d7-4960-91bf-143e914b1719', 'markdown', '')
                print 'msg长度为:%i\n' % len(msg.encode('utf-8')), msg
                msg = ''
        msg += "## 数据波动汇总见[grafana](%s)" % get_grafana_url(grafana_info, hour)
        if index > 1:
            wetchat_robot(msg, '30afa32f-a0d7-4960-91bf-143e914b1719', 'markdown', '')
        print 'msg长度为:%i\n' % len(msg.encode('utf-8')), msg