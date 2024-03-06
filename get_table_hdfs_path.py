# coding: utf-8
import os
import sys
import requests
import json
from pyspark.sql import SparkSession
import time
from datetime import datetime
import http.client
import pandas as pd


def parse_ymd(s):
    year_s, mon_s, day_s = s.split('-')
    return datetime(int(year_s), int(mon_s), int(day_s))


def get_data_from_atlas(url, data, type):
    headers = {'Content-Type': 'application/json',
               'Authorization': 'Basic ZGF0YXdhcmVob3VzZTpkYXRhd2FyZWhvdXNlQGJpZ28xMjM='}
    global atlas_num
    if type == 'post':
        res = requests.post(url, data=json.dumps(data), headers=headers, timeout=150, verify=False)
        atlas_num += 1
    elif type == 'get':
        res = requests.get(url=url, headers=headers, timeout=150)
        atlas_num += 1
    else:
        raise Exception('type error!')
    retry_cnt = 1
    while retry_cnt <= 3 and res.status_code != 200:
        print('retry_cnt:%i, atlas_num:%s' % (retry_cnt, atlas_num))
        retry_cnt += 1
        time.sleep(3)
        if type == 'post':
            res = requests.post(url, data=json.dumps(data), headers=headers, timeout=150, verify=False)
        elif type == 'get':
            res = requests.get(url=url, headers=headers, timeout=150)
        else:
            raise Exception('type error!')
    if res.status_code != 200:
        print('res.text:', res.text)
        res.close()
        raise Exception(res)
    return json.loads(res.text)


def get_table_info(db_set, table_set):
    table_dict = dict()
    # 因受限于复杂查询条件个数，分db多次查询
    for db in db_set:
        base_url = 'http://onemeta.bigdata.bigo.inner/api/atlas/v2/hive_table/basicSearch'
        base_data = {'query': '%s.' % db,
                     'typeName': 'hive_table',
                     'limit': 10000,
                     'offset': 0,
                     'excludeDeletedEntities': 1,
                     'sortBy': 'fileSize',
                     'sortOrder': 'DESCENDING'
                     }
        atlas_data = get_data_from_atlas(url=base_url, data=base_data, type='post')
        for d in atlas_data.get("data"):
            table_name = d.get('dbName') + '.' + d.get('tableName')
            guid = d.get('guid', '')
            abnormal_info = 'normal'
            if table_name in table_set:
                table_size = d.get('fileSize', 0)
                query_times = d.get('queryTimes', 0)
                if table_size > 0:
                    update_date = time.strftime('%Y-%m-%d', time.localtime(int(d.get('dataUpdateTime')) / 1000))
                    diff = (parse_ymd(day) - parse_ymd(update_date)).days
                    # diff超过2天才判定为断流，因为atlas的更新时间一般晚1天
                    if diff > 100:
                        abnormal_info = 'TaskStop100+Days'
                    elif diff >= 2:
                        abnormal_info = 'TaskStop%sDays' % diff
                else:
                    abnormal_info = 'EmptyTable'
                if guid == '':
                    abnormal_info = 'NotFound'
                table_dict[table_name] = [abnormal_info, table_size, query_times]
    # 获取所有表的分区及路径信息
    get_all_url = 'http://onemeta.bigdata.bigo.inner/api/atlas/v2/hive_table'
    all_info = get_data_from_atlas(url=get_all_url, data=[], type='get')
    for info in all_info['tables']:
        table_name = info.get('db') + '.' + info.get('tablename')
        if table_name in table_dict:
            hdfs_path = info.get('location').replace('hdfs://bigocluster', '') if info.get('location') else ''
            # 修正hdfs路径获取错误的bug
            if hdfs_path.split('/')[-1] != info.get('tablename') and (hdfs_path.endswith('tmp2') or hdfs_path.endswith('tmp1') or hdfs_path.endswith('tmp')):
                print('hdfs error:', hdfs_path, info.get('tablename'))
                hdfs_path = hdfs_path.replace(hdfs_path.split('/')[-1], info.get('tablename'))
            table_partition = info.get('partitionKeys') if info.get('partitionKeys') else ''
            table_dict[table_name].extend([hdfs_path, table_partition])
    return table_dict



def get_latest_10_days_imo_us_info(day):
    # imo_us需要监控的数据的访问次数并不稳定，故取过去10天有过访问的表，继续进行count操作，但是把查询次数定义为0
    sql_str = """
                    select
                    tb_name
                    from dwd.count_hdfs_size_di
                    where day between date_sub('%s', 11) and date_sub('%s', 1) 
                    and tb_source='imo_us' and query_times>1
                    group by tb_name
                    """ % (day, day)
    sparkSession = SparkSession.builder.appName('get_latest_10_days_imo_us_info').enableHiveSupport().getOrCreate()
    hive_ret = sparkSession.sql(sql_str)
    ret = hive_ret.collect()
    res = {}
    for r in ret:
        if r.tb_name not in res:
            res[r.tb_name] = 0
    print('get_latest_10_days_imo_us_info len:',len(res))
    return res

def count_imo_us_table_size(day):
    print('count_imo_us_table_size_begin(%s):\n' % day)
    task_dict = count_imo_us_task_num(day)
    latest_10_days_info = get_latest_10_days_imo_us_info(day)
    for k, v in latest_10_days_info.items():
        if k not in task_dict:
            task_dict[k] = v
    table_dict = dict()
    # 查表大小
    du_cmd = "hadoop fs -du -s hdfs://sg-nn2.bigdata.bigo.inner:8888/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/*|awk -F' ' '{print $1,$2,$3}'"
    with os.popen(du_cmd, 'r') as p:
        stdout = p.read().strip()
    info_list = stdout.split('\n')
    for info in info_list:
        size1, size2, full_path = info.split(' ')
        tb_name = full_path.split('imo_data_logs/')[1].split('/day=')[0]
        # 0访问的表需要大于10G
        if (tb_name in task_dict or (tb_name not in task_dict and int(size1) >= 10*1024*1024*1024)) and tb_name not in table_dict:
            table_dict[tb_name] = {'table_size': int(size1), 'query_times': 0}
        if tb_name in table_dict and tb_name in task_dict:
            table_dict[tb_name]['query_times'] = task_dict[tb_name]
    res = list()
    for k, v in table_dict.items():
        # (r.tb_name, r.event_id, r.tb_source, r.hdfs_path, r.abnormal_info, r.table_size, r.query_times)
        res.append([k, '', 'imo_us', '/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/%s' % k, 'normal', v['table_size'], v['query_times'], 'day'])
    print('count_imo_us_size_day_end')
    return res


def count_imo_us_task_num(day):
    conn = http.client.HTTPConnection("sg.clickhouse.bigo.inner")
    payload_dict = {"sql":"",  "database" : "default", "user" : "pandeng", "password" : "b59b9948", "cluster" : "s2"}
    payload_dict['sql'] = """
            select 
            splitByChar('/',hdfs_path)[-1] as tb_name
            ,count(distinct app_id) as num
            from
            (
                select 
                splitByString('/day=', src)[1] as hdfs_path
                ,concat('application_',splitByChar('_',splitByString('application_', proto)[2])[1],'_',splitByChar('_',splitByString('application_',proto)[2])[2]) as app_id
                from default.sg_bigocluster_hdfs_audit_log_all
                where day=date_sub(day, 1, toDate('%s'))
                and src like '%%/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/%%'
                and proto like '%%application_%%'  
                group by 
                hdfs_path,app_id
                union all
                select 
                splitByString('/day=', src)[1] as hdfs_path
                ,concat('application_',splitByChar('_',splitByString('application_', proto)[2])[1],'_',splitByChar('_',splitByString('application_',proto)[2])[2]) as app_id
                from default.sgcold_hdfs_audit_log_all
                where day BETWEEN date_sub(day, 1, toDate('%s')) and '%s'
                and src like '%%/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/%%'
                and proto like '%%application_%%'  
                group by 
                hdfs_path,app_id
            ) t1 where app_id GLOBAL not in (
                select id from default.bigocluster_app_info_sg_all
                where day BETWEEN date_sub(day, 1, toDate('%s')) and '%s' and user='amy'
                group by id
            )
            and app_id GLOBAL not in
            (select appId from default.sg_spark_metric_new_all
                where day BETWEEN date_sub(day, 1, toDate('%s')) and '%s' and user='amy'
                group by appId
            )
            group by tb_name
    """ % (day, day, day,day,day,day,day)
    payload_str = json.dumps(payload_dict)
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
    }
    conn.request("POST", "/api/query/sql", payload_str, headers)
    res = conn.getresponse()
    data = res.read()
    data_list = data.decode("utf-8").strip().split('\n')
    res = dict()
    print(data_list)
    for d in data_list:
        if '\t' in d:
            tb_name, num = d.split('\t')
            if tb_name not in res:
                res[tb_name] = int(num)
    print('count_imo_us_task_num len:', len(res))
    return res


if __name__ == '__main__':
    # 获取所有需要监控的hive表及事件id
    day = sys.argv[1]
    # 记录单次调度执行的总访问次数
    atlas_num = 0
    global day, atlas_num
    # table_name = sys.argv[2]
    # 不再对tiki及小流量/小价值db库下的流量表进行监控
    sql_str = """
                select 
                t1.tb_name
                ,t1.tb_source
                ,t1.event_id
                from
                (
                    select
                    trim(lower(concat(hive_db,'.',hive_table))) as tb_name
                    ,'bdp' as tb_source
                    ,'' as event_id
                    ,'' as hdfs_path
                    from common.sg_owl_service_adm_oss_hive_exp_tbl_task_orc
                    where task_type=3 and dest_cluster_id=1 and status=3 and area='sg' 
                    and hive_db!='tmp' 
                    group by trim(lower(concat(hive_db,'.',hive_table)))
                    union all
                    SELECT
                    trim(lower(hive_table)) as tb_name
                    ,'normal_event' as tb_source
                    ,'' as event_id
                    ,'' as hdfs_path
                    from common.sg_owl_service_adm_oss_cfg_event_orc
                    where is_common = 0 and hive_table!=''
                    group by trim(lower(hive_table))
                    union all 
                    select 
                    tb_name
                    ,tb_source
                    ,event_id
                    ,hdfs_path
                    from dwd.user_event_table_info
                    group by 
                    tb_name
                    ,tb_source
                    ,event_id
                    ,hdfs_path
                ) t1
                where split(t1.tb_name, '.')[0] not in ('test','com_e_commerce_eu','likee_prod','livelite','bigdata_frontend'
                                     ,'default','woodpecker','liggs_bigwin','dak','ofree','vpn','flipped',
                                     'likee_wall_paper','bigoface','ksing','cupid','cubetv')
                      and t1.tb_name not regexp 'tiki' and t1.tb_name not rlike '[\u4e00-\u9fa5]'
                """
    sparkSession = SparkSession.builder.appName('get_table_hdfs_path_di').enableHiveSupport().getOrCreate()
    hive_ret = sparkSession.sql(sql_str)
    ret = hive_ret.collect()
    db_set = set()
    table_set = set()
    table_list = list()
    for r in ret:
        db_set.add(r.tb_name.split('.')[0])
        table_set.add(r.tb_name)
        table_list.append([r.tb_name, r.tb_source, r.event_id])
    print(db_set)
    table_dict = get_table_info(db_set, table_set)
    to_monitor = list()
    print('atlas_num:', atlas_num)
    for table_info in table_list:
        tb_name, tb_source, event_id = table_info
        if tb_name in table_dict:
            abnormal_info, table_size, query_times, hdfs_path, table_partition = table_dict[tb_name]
            if 'NotFound' == abnormal_info:
                print('table %s NotFound' % tb_name)
            to_monitor.append([tb_name, event_id, tb_source, hdfs_path, abnormal_info, table_size, query_times, table_partition])
    to_monitor.extend(count_imo_us_table_size(day))
    pd_df = pd.DataFrame(to_monitor, columns=['tb_name', 'event_id','tb_source',  'hdfs_path', 'abnormal_info',
                                             'table_size', 'query_times', 'table_partition'])
    spark_df = sparkSession.createDataFrame(pd_df)
    spark_df.createOrReplaceTempView('get_table_hdfs_path_di_view')
    insert_sql = "insert overwrite table dwd.get_table_hdfs_path_di select * from get_table_hdfs_path_di_view"
    sparkSession.sql(insert_sql)
