# coding: utf-8
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql import SparkSession
from datetime import datetime,timedelta
import json
import requests
import time
import re
import pandas as pd


# 提前在sg和hello集群建立的人工维护表与字段信息表
"""
create table if not exists dwd.monitor_column_business_table(
 app_name string COMMENT '业务线',
 table_name string COMMENT '表名',
 table_source string COMMENT '表来源描述'
 )
COMMENT '进行指标监控的业务表-人工维护(sg/hello集群)'
ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 ESCAPED BY '\\'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE ;
 
create table if not exists dwd.monitor_column_info_di
(
  app_name string comment '业务名'
  ,table_name string comment '表名'
  ,table_source string comment '待监控表来源的中文描述'
  ,table_owner string comment '表归属人'
  ,table_status string comment '表更新时间'
  ,table_partition string comment '表分区信息,多个分区用逗号分隔'
  ,table_size bigint comment '表大小'
  ,query_times int comment '表昨日访问次数'
  ,day_size bigint comment '最近10日数据量均值,作为后续拼接sql的参考'
  ,downstream_user string comment '下游业务用户,用逗号分隔,只对数仓业务线有价值'
  ,column_name string comment '字段名'
  ,column_type string comment '字段类型'
  ,column_comment string comment '字段注释'
) comment '字段信息全量表,用于指标监控'
partitioned by
(
    day string comment '天分区'
)
stored as orc
tblproperties (
  'orc.compress'='snappy'
);
"""

def parse_ymd(s):
    year_s, mon_s, day_s = s.split('-')
    return datetime(int(year_s), int(mon_s), int(day_s))


def get_data_from_atlas(url, data, type):
    # 一秒内发送请求不要超过3次
    time.sleep(0.5)
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
        print('retry_cnt:', retry_cnt)
        retry_cnt += 1
        time.sleep(1)
        if type == 'post':
            res = requests.post(url, data=json.dumps(data), headers=headers, timeout=150, verify=False)
            atlas_num += 1
        elif type == 'get':
            res = requests.get(url=url, headers=headers, timeout=150)
            atlas_num += 1
        else:
            raise Exception('type error!')
    if res.status_code != 200:
        raise Exception('Is atlas_num %i overflow' % atlas_num)
    return json.loads(res.text)


def collect_sg_dw_table(day):
    base_url = 'http://onemeta.bigdata.bigo.inner/api/atlas/v2/hive_table/advancedSearch'
    base_data = {
        "typeName": "hive_table",
        "sortBy": "queryTimes",
        "excludeDeletedEntities": 1,
        "includeClassificationAttributes": 0,
        "includeSubTypes": 1,
        "includeSubClassifications": 1,
        "limit": 5000,
        "offset": 0,
        "ignoreLocation": 1,
        "queryFilters": {
            "attributeName": "null",
            "attributeValue": "null",
            "condition": "OR",
            "criterion": [
                {
                    "attributeName": "null",
                    "operator": "contains",
                    "attributeValue": "dws."
                },
                {
                    "attributeName": "null",
                    "operator": "contains",
                    "attributeValue": "dwd."
                },
                {
                    "attributeName": "null",
                    "operator": "contains",
                    "attributeValue": "dim."
                },
                {
                    "attributeName": "null",
                    "operator": "contains",
                    "attributeValue": "app."
                },
                {
                    "attributeName": "null",
                    "operator": "contains",
                    "attributeValue": "live_dw_"
                },
                {
                    "attributeName": "null",
                    "operator": "contains",
                    "attributeValue": "like_dw_"
                }
            ]
        },
        "sortOrder": "DESCENDING"
    }
    atlas_data = get_data_from_atlas(url=base_url, data=base_data, type='post')
    res = []
    for d in atlas_data.get("data"):
        # 限定库名表名，临时表,tiki业务线不监控
        if re.search('dws|app|dim|dwd|live_dw_|like_dw_', d.get('dbName')) and not re.search('temp|tmp|tiki' ,d.get('tableName')):
            table_name = d.get('dbName') + '.' + d.get('tableName')
            table_size = long(d.get('fileSize', 0))
            query_times = int(d.get('queryTimes', 0))
            if d.get('dataUpdateTime', 0) and d.get('dataUpdateTime', 0)>0 and table_size>0 and query_times>0:
                update_date = time.strftime('%Y-%m-%d', time.localtime(int(d.get('dataUpdateTime', 0)) / 1000))
                diff = (parse_ymd(day) - parse_ymd(update_date)).days
                # diff超过2天才判定为断流，因为atlas的更新时间一般晚1天
                if diff <= 2:
                    res.append(table_name)
    return res


def get_all_table_columns_info(host,table_dict,day):
    # 获取所有表的分区及路径信息
    get_all_url = '%s/api/atlas/v2/hive_table' % host
    all_info = get_data_from_atlas(url=get_all_url, data=[], type='get')
    for info in all_info['tables']:
        table_name = info.get('db') + '.' + info.get('tablename')
        if table_name in table_dict:
            app_name, table_source = table_dict[table_name]
            hdfs_path = info.get('location').replace('hdfs://bigocluster', '') if info.get('location') else ''
            table_partition = info.get('partitionKeys').lower() if info.get('partitionKeys') else ''
            guid = info.get('guid')
            basic_url = '%s/api/atlas/v2/hive_table/basic?guid=%s' % (host, guid)
            basic_data = get_data_from_atlas(url=basic_url, data=[], type='get')
            table_size = long(basic_data.get('fileSize', 0))
            query_times = int(basic_data.get('queryTimes', 0))
            owner = basic_data.get("owner")
            realOwner = basic_data.get("realOwner")
            table_owner = realOwner if realOwner and realOwner != '' else owner
            update_date = time.strftime('%Y-%m-%d', time.localtime(int(basic_data.get('dataUpdateTime', 0)) / 1000))
            table_status = time.strftime('%H:%M:%S', time.localtime(int(basic_data.get('dataUpdateTime', 0)) / 1000))
            diff = (parse_ymd(day) - parse_ymd(update_date)).days
            # diff超过2天才判定为断流,因为atlas的更新时间一般晚1天
            if diff > 2:
                print table_name, 'TaskStop%sDays!' % diff
            detail_url = '%s/api/atlas/v2/hive_table/detail?guid=%s' % (host, guid)
            res_detail = get_data_from_atlas(url=detail_url, data=[], type='get')
            # 获取下游血缘信息
            lineage_url = '%s/api/atlas/v2/lineage/list?guid=%s&depth=1&direction=OUTPUT' % (host, guid)
            res_lineage = get_data_from_atlas(url=lineage_url, data=[], type='get')
            lineage = set()
            for l in res_lineage['data']:
                if l['depth'] == 1 and l['updateTime']:
                    update_date2 = time.strftime('%Y-%m-%d',time.localtime(int(l['updateTime']) / 1000))
                    diff2 = (parse_ymd(day) - parse_ymd(update_date2)).days
                    # 排除临时任务
                    if l['owner'] and diff2 <= 2:
                        lineage.add(l['owner'].lower())
            downstream_user = ','.join(list(lineage))
            columns_info = []
            for columns in res_detail['columns']:
                columns_info.append((columns['name'], columns['type'], columns['comment'] if columns['comment'] else ''))
            day_before_7 = (parse_ymd(day) - timedelta(7)).strftime('%Y-%m-%d')
            day_size_url = '%s/api/atlas/v2/hive_table/partitionTrend/%s?fromDay=%s&toDay=%s&field=fileSize' % (
            host, guid, day_before_7, day)
            res_day_size = get_data_from_atlas(url=day_size_url, data=[], type='get')
            day_size, day_size_total = 0, 0
            for info in res_day_size:
                day_size_total += info['value']
            # 取最近N天的平均值,小时数据量粗略取其24分之一，因为只是作为后续拼接sql的参考
            # 如果天分区大小获取为空，说明该表分区比较特殊，需要根据hdfs命令获取计算
            if len(res_day_size) > 0:
                day_size = long(day_size_total / len(res_day_size))
            else:
                # 含有天分区的表
                if 'day' in table_partition and hdfs_path != '':
                    for p in table_partition.split(','):
                        if p != 'day':
                            hdfs_path += '/*'
                        else:
                            hdfs_path += '/day=%s' % (parse_ymd(day) - timedelta(1)).strftime('%Y-%m-%d')
                    du_cmd = "hadoop fs -du hdfs://sg-nn2.bigdata.bigo.inner:8888%s|awk -F' ' '{print $1}'" % hdfs_path
                    with os.popen(du_cmd, 'r') as p:
                        du_out = p.read().strip()
                    # du为空时,表示该天分区为0,可能是按天更新的月分区表,比如report_tb.ads_tiki_country_natural_mau_2ck
                    # 这类表当命中月初或月末时才监控（待定，因为超过了30天的样本范围，考虑人工去除）
                    if du_out == '':
                        day_size = 0
                    else:
                        for du_info in du_out.split('\n'):
                            if du_info != '':
                                day_size += int(du_info)
                # 无天分区，当成全量表处理
                else:
                    day_size = table_size
            if 'day' == table_partition:
                ls_cmd = "hadoop fs -ls hdfs://sg-nn2.bigdata.bigo.inner:8888%s|awk -F' ' '{print $8}'" % hdfs_path
                with os.popen(ls_cmd, 'r') as p:
                    stdout = p.read().strip()
                month = '-'.join(day.split('-')[:-1])
                max_day = day
                for out in stdout.split('\n'):
                    if 'day=' in out:
                        date_tmp = out.split('day=')[1]
                        # 排除非日期分区
                        if month in date_tmp:
                            max_day = date_tmp
                diff3 = (parse_ymd(day) - parse_ymd(max_day)).days
                # T+1更新的表，第二天使用时，天分区也需要+1,则是当前day
                if diff3 <= 1:
                    table_partition += ';%s' % day
                else:
                    yesterday = (parse_ymd(day) - timedelta(1)).strftime('%Y-%m-%d')
                    # T+2更新的表
                    table_partition += ';%s' % yesterday
            if (downstream_user == '' and app_name == 'dw') or diff > 2:
                del table_dict[table_name]
            else:
                table_dict[table_name].extend([table_owner, table_status, table_partition,downstream_user, table_size, query_times, day_size, columns_info])


if __name__ == '__main__':
    day = sys.argv[1]
    cluster = sys.argv[2]
    global atlas_num
    atlas_num = 0
    if cluster == 'sg':
        host = 'http://onemeta.bigdata.bigo.inner'
        dw_table = collect_sg_dw_table(day)
        table_str = "('" + "',\n'".join(dw_table) + "')"
        print table_str
        sql_str = """
        select 
        app_name
        ,table_name
        ,table_source
        from
        (
            select 
            app_name
            ,t1.table_name
            ,table_source
            ,row_number() over (partition by t1.table_name order by prefer desc) as rn
            from
            (
                select
                'dw' as app_name
                ,ql as table_name
                ,'dw' as table_source
                ,1 as prefer
                from
                (
                SELECT user_name
                ,regexp_replace(regexp_replace(LOWER(query), '--.*?\n', ' '), '(\n|\t)', ' ') as query
                ,split(regexp_replace(regexp_replace(LOWER(query), '--.*?\n', ' '), '(\n|\t)', ' '), ' ') as query_list
                from bigolive.sparksql_job_audit_flume
                where job_state='FINISHED' and day=date_sub('%s', 1)
                and user_name not in ('hadoop','hive_dw1','hive_dw2','faye','caizhiwei','chenyingying','chenyingying.chen','liuyadong','pandeng','shangchunlei','zane')
                ) t1 LATERAL VIEW explode(query_list) t2 AS ql
                where ql in %s
                group by ql
                union all
                select app_name,table_name,table_source,2 as prefer from dwd.monitor_column_business_table
            ) t1 
            left join 
            (select table_name from dwd.monitor_column_whitelist_all group by table_name) t2 on t1.table_name=t2.table_name
            where t2.table_name is null  
        ) t where rn=1
        """ % (day, table_str)
    elif cluster == 'hello':
        host = 'http://onemeta520.bigdata.bigo.inner'
        # hello集群目前无数仓表,只取hello集群的业务表即可
        sql_str = "select app_name,table_name,table_source from dwd.monitor_column_business_table"
    else:
        raise Exception('cluster is not exists!')
    sparkSession = SparkSession.builder.appName('get_new_table').enableHiveSupport().getOrCreate()
    hive_ret = sparkSession.sql(sql_str)
    ret = hive_ret.collect()
    data_list = []
    table_dict = dict()
    for r in ret:
        app_name = r.app_name
        table_name = r.table_name
        table_source = r.table_source
        if table_name not in table_dict:
            table_dict[table_name] = [app_name, table_source]
    print 'before:'
    print table_dict
    get_all_table_columns_info(host, table_dict, day)
    print 'after:'
    print table_dict
    for k, v in table_dict.items():
        table_name = k
        app_name, table_source, table_owner, table_status, table_partition,downstream_user, table_size, query_times, day_size, columns_info = v
        if day_size > 0:
            for col in columns_info:
                data_list.append([app_name, table_name, table_source, table_owner, table_status, table_partition, table_size,
                    query_times, day_size, downstream_user, col[0],col[1], col[2]])
        else:
            print 'not monitor table info:', table_name, table_status, table_size, query_times, day_size
    pd_df = pd.DataFrame(data_list,columns=['app_name', 'table_name', 'table_source', 'table_owner', 'table_status',
                                  'table_partition', 'table_size','query_times', 'day_size', 'downstream_user',
                                  'column_name', 'column_type','column_comment'])
    spark_df = sparkSession.createDataFrame(pd_df)
    spark_df.createOrReplaceTempView('monitor_column_info_di_view')
    insert_sql = "insert overwrite table dwd.monitor_column_info_di partition(day='%s') select * from monitor_column_info_di_view" % day
    sparkSession.sql(insert_sql)
    print 'atlas_num is %i' % atlas_num



