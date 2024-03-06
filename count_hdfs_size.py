# coding: utf-8
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql import SparkSession
import multiprocessing
from datetime import datetime
import re
import pandas as pd


# 临时需求，收集表的ddl信息
def get_table_ddl_info(table_name):
    import json
    import requests
    get_guid_url = 'http://onemeta.bigdata.bigo.inner/api/atlas/v2/hive_table/basicSearch'
    get_guid_data = {'query': '%s' % table_name,
                     'typeName': 'hive_table',
                      'limit': 10,
                      'offset': 0,
                      'sortBy': 'fileSize',
                      'sortOrder': 'ASCENDING'
                     }
    headers = {'Content-Type': 'application/json', 'Authorization': 'Basic cmFuZ2VydGFnc3luYzpyYW5nZXJAYmlnbzEyMw=='}
    res = requests.post(get_guid_url, data=json.dumps(get_guid_data), headers=headers, timeout=20, verify=False)
    json_resp = json.loads(res.text)
    # print 'name,hive_table,创建人,负责人,描述,业务线,类目,权限等级,ttl,路径,文件总大小'
    if len(json_resp.get("data")) > 0:
        for d in json_resp.get("data"):
            guid = d.get("guid")
            if guid != '':
                if d.get('dbName') + '.' + d.get('tableName') == table_name:
                    # 获取下游血缘信息
                    lineage_url = 'http://onemeta.bigdata.bigo.inner/api/atlas/v2/lineage/list?guid=%s&depth=1&direction=OUTPUT' % guid
                    res_lineage = get_data_from_atlas(url=lineage_url, data=[], type='get')
                    lineage = set()
                    for l in res_lineage['data']:
                        if l['depth'] == 1 and l['updateTime']:
                            update_date2 = time.strftime('%Y-%m-%d', time.localtime(int(l['updateTime']) / 1000))
                            diff2 = (parse_ymd(day) - parse_ymd(update_date2)).days
                            # 排除临时任务
                            if l['name'] and diff2 <= 2:
                                lineage.add(l['name'])
                    print '%s,%d,%s' % (table_name, len(lineage), '|'.join(list(lineage)))
                    break
                else:
                    continue
            else:
                print '%s,guid is null,' % table_name
    else:
        print '%s,table not found,' % table_name
                    # downstream_user = ','.join(list(lineage))
                    # get_hdfs_url = 'http://onemeta.bigdata.bigo.inner/api/atlas/v2/hive_table/basic?guid=%s' % guid
                    # res2 = requests.get(url=get_hdfs_url, headers=headers, timeout=20)
                    # hdfs_path, = json.loads(res2.text).get('location')
                    # get_guid_url3 = 'http://onemeta.bigdata.bigo.inner/api/atlas/v2/hdfs_path/basicSearch'
                    # get_guid_data3 = {'query': '%s' % d.get('tableName'),
                    #                   'typeName': 'hdfs_path',
                    #                   'limit': 10,
                    #                   'offset': 0,
                    #                   'sortBy': 'fileSize',
                    #                   'sortOrder': 'ASCENDING'
                    #                   }
                    # res3 = requests.post(get_guid_url3, data=json.dumps(get_guid_data3), headers=headers, timeout=20,
                    #                      verify=False)
                    # json_resp3 = json.loads(res3.text)
                    # if len(json_resp3.get("data")) > 0:
                    #     for d3 in json_resp3.get("data"):
                    #         if d3.get("path") == hdfs_path:
                    #             diskUsed = d3.get("diskUsed")
                    #             break
                    #         else:
                    #             diskUsed = -1
                    # else:
                    #     diskUsed = -1
                    # # print '%s;%s.%s;%s;%s;%s;%s;%s;%s;%s;%s;%s' %(table_name, d.get("dbName"), d.get("tableName"), d.get("owner"),d.get("realOwner"),d.get("comment")
                    # #                                               ,d.get("businessAttributes").get("businessLine").get("app")
                    # #                                               ,d.get("businessAttributes").get("tableClassification",{'cOne':'None'}).get("cOne")
                    # #                                               ,d.get("businessAttributes").get("businessLine").get("accessLevel")
                    # #                                               ,d.get("ttl"),hdfs_path,diskUsed)
                    # print diskUsed


def parse_ymd(s):
    year_s, mon_s, day_s = s.split('-')
    return datetime(int(year_s), int(mon_s), int(day_s))


def count_imo_us_size_hour(day, hour, imo_us_info):
    print 'count_imo_us_size_hour_begin(%s-%s):\n' % (day, hour)
    ls_cmd = "hadoop fs -ls hdfs://sg-nn2.bigdata.bigo.inner:8888/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/*/day=%s|awk -F' ' '{print $5,$7,$8}'" % day
    with os.popen(ls_cmd, 'r') as p:
        stdout = p.read().strip()
    info_list = stdout.split('\n')
    table_dict = dict()
    for info in info_list:
        if len(info) > 3 and '.inprogress.' not in info:
            size, update_time, hdfs_path = info.split(' ')
            tb_name = hdfs_path.split('imo_data_logs/')[1].split('/day=')[0]
            hdfs_path = '/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/' + tb_name + '/day=' + day
            if tb_name in imo_us_info and tb_name not in table_dict:
                table_dict[tb_name] = {'file_size': 0, 'hdfs_path': hdfs_path,'abnormal_info': 'normal'
                    ,'table_size': imo_us_info[tb_name][0], 'query_times': imo_us_info[tb_name][1]}
            if tb_name in table_dict and update_time.split(':')[0] == hour:
                table_dict[tb_name]['file_size'] += long(size)
    for k, v in imo_us_info.items():
        if k not in table_dict:
            table_dict[k] = {'file_size': 0, 'hdfs_path': '/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/' + k,
                             'abnormal_info': count_imo_us_cut_off_day(day, k), 'table_size': v[0], 'query_times': v[1]}
    res = list()
    for k, v in table_dict.items():
        # tb_name,event_id,tb_source,hdfs_path,file_size,abnormal_info, day, hour
        res.append([k, '', 'imo_us', v['hdfs_path'], long(v['file_size']), v['abnormal_info'],long(v['table_size']), v['query_times'], 'day', day, hour])
    print 'count_imo_us_size_hour_end:\n%s' % len(res)
    return res


def count_imo_us_cut_off_day(day, tb_name):
    first_cut_off_day = '2100-01-01'
    du_cmd_single = "hadoop fs -du hdfs://sg-nn2.bigdata.bigo.inner:8888/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/%s|awk -F' ' '{print $1,$2,$3}'" % tb_name
    with os.popen(du_cmd_single, 'r') as p:
        du_out = p.read().strip()
    du_info = du_out.split('\n')
    abnormal_info, size1, update_date = 'normal', 0, ''
    # 只显示最后110个分区，diff大于100就标识为断流100+，否则标识为实际断流天数
    try:
        for info2 in du_info[-110:]:
            if len(info2) > 3:
                size1, size2, full_path = info2.split(' ')
                if 'day=' in full_path:
                    update_date = full_path.split('day=')[1].split('/')[0]
                else:
                    continue
                if int(size1) != 0 and update_date < day:
                    # 如果前面有数据为0的day分区，后面又出现了数据不为0的day分区，需要初始化最早断流日期
                    first_cut_off_day = '2100-01-01'
                # 断流场景
                elif int(size1) == 0 and update_date <= day:
                    if update_date < first_cut_off_day:
                        print 'new first_cut_off_day:', update_date
                        first_cut_off_day = update_date
                        diff = (parse_ymd(day) - parse_ymd(update_date)).days
                        if diff >= 100:
                            abnormal_info = 'CutOff100+Days'
                        else:
                            abnormal_info = 'CutOff%iDays' % (diff + 1)
        # 循环结束时，size不为0，但是最后一条数据的update_date仍小于day时，属于调度停更，需要更新异常
        if size1 != 0 and update_date < day and abnormal_info == 'normal':
            diff = (parse_ymd(day) - parse_ymd(update_date)).days
            if diff >= 100:
                abnormal_info = 'TaskStop100+Days'
            else:
                abnormal_info = 'TaskStop%iDays' % (diff + 1)
    except:
        print 'du_cmd_single:', du_cmd_single
        print tb_name, ' update_date is error:', update_date
        abnormal_info = 'update_date error'
    return abnormal_info


def count_imo_us_size_day(day, imo_us_info):
    print 'count_imo_us_size_day_begin(%s):\n' % day
    table_dict = dict()
    du_cmd_all = "hadoop fs -du -s hdfs://sg-nn2.bigdata.bigo.inner:8888/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/*/day=%s|awk -F' ' '{print $1,$2,$3}'" % day
    with os.popen(du_cmd_all, 'r') as p:
        stdout = p.read().strip()
    info_list = stdout.split('\n')
    for info in info_list:
        if len(info) > 3 and '.inprogress.' not in info:
            size1, size2, full_path = info.split(' ')
            tb_name = full_path.split('imo_data_logs/')[1].split('/day=')[0]
            hdfs_path = full_path.replace('hdfs://sg-nn2.bigdata.bigo.inner:8888', '')
            if tb_name in imo_us_info and tb_name not in table_dict:
                table_dict[tb_name] = {'file_size': long(size1), 'hdfs_path': hdfs_path,'abnormal_info': 'normal'
                    ,'table_size': imo_us_info[tb_name][0], 'query_times': imo_us_info[tb_name][1]}
                print '1:',size1, size2, full_path
            # 先找出昨日已经断流的表，然后再更新断流日期，并进行标识
            if tb_name in table_dict and long(size1) == 0:
                table_dict[tb_name]['abnormal_info'] = count_imo_us_cut_off_day(day, tb_name)
    for k, v in imo_us_info.items():
        if k not in table_dict:
            table_dict[k] = {'file_size': 0, 'hdfs_path': '/data/apps/imo_us/imo_data_logs_etl/data/imo_data_logs/'+k,
                             'abnormal_info': count_imo_us_cut_off_day(day, k), 'table_size': v[0], 'query_times': v[1]}
    res = list()
    for k, v in table_dict.items():
        res.append([k, '', 'imo_us', v['hdfs_path'], long(v['file_size']), v['abnormal_info'], long(v['table_size']), v['query_times'], 'day', day])
    print 'count_imo_us_size_day_end:\n%s' % len(res)
    return res


def count_hdfs_size_day(info_list):
    print 'count_hdfs_size_day_begin:', info_list
    tb_name, event_id, tb_source, hdfs_path, abnormal_info,table_size, query_times, table_partition, day, hour = info_list
    file_size,table_size_new, update_date, update_time, bdp_have_day_partition = 0, 0, '','', False
    if abnormal_info == 'normal' or ('TaskStop' in abnormal_info and int(re.findall(r"\d+",abnormal_info)[0]) < 100):
        if tb_source == 'bdp':
            if table_partition == '':
                ls_cmd = "hadoop fs -ls hdfs://sg-nn2.bigdata.bigo.inner:8888%s|awk -F' ' '{print $5,$6,$7,$8}'" % hdfs_path
                with os.popen(ls_cmd, 'r') as p:
                    stdout = p.read().strip()
                # 普通全量表的处理
                info_list = stdout.split('\n')
                for info in info_list:
                    if len(info) > 3 and '.inprogress.' not in info:
                        size, update_date, update_time, full_path = info.split(' ')
                        file_size += long(size)
                    # 导表全量表的更新时间可能小于等于当前日期，属于断流的场景
                    diff = (parse_ymd(day) - parse_ymd(update_date)).days
                    if diff >= 100:
                        abnormal_info = 'CutOff100+Days'
                    # diff为0，9点后更新的全量表，不算断流
                    elif diff > 0 or (diff == 0 and update_time <= '09:00'):
                        abnormal_info = 'CutOff%iDays' % (diff + 1)
                    else:
                        abnormal_info = 'normal'
            elif 'mon' in table_partition:
                abnormal_info = 'MonthPartition'
            elif 'day' in table_partition:
                bdp_have_day_partition = True
            else:
                raise Exception('bdp have partition %s' % table_partition)
        # 普通打点表与bdp有day分区表的处理
        if tb_source == 'normal_event' or bdp_have_day_partition:
            du_cmd = "hadoop fs -du hdfs://sg-nn2.bigdata.bigo.inner:8888%s|awk -F' ' '{print $1,$2,$3}'" % hdfs_path
        elif tb_source == 'common_event':
            du_cmd = "hadoop fs -du -s hdfs://sg-nn2.bigdata.bigo.inner:8888%s/day=*/hour=*/event_id=%s|awk -F' ' '{print $1,$2,$3}'" % (hdfs_path, event_id)
        else:
            # bdp全量表已处理完毕，可直接返回结果
            print 'count_hdfs_size_end:', [tb_name, event_id, tb_source, hdfs_path, file_size, abnormal_info,table_size,query_times,table_partition, day]
            return [tb_name, event_id, tb_source, hdfs_path, long(file_size), abnormal_info,long(table_size),query_times,table_partition, day]
        with os.popen(du_cmd, 'r') as p:
            du_out = p.read().strip()
        du_info = du_out.split('\n')
        if len(du_info) > 0 and du_info[0] != '':
            first_cut_off_day = '2100-01-01'
            first_cut_off_hdfs_path = hdfs_path
            for info in du_info:
                if len(info) > 3 and '.inprogress.' not in info:
                    size, size2, full_path = info.split(' ')
                    # 有些临时文件不带day,需要排除
                    if 'day=' in full_path:
                        update_date = full_path.split('day=')[1].split('/')[0]
                    else:
                        continue
                    if int(size) != 0 and update_date < day:
                        table_size_new += int(size)
                        # 如果前面有数据为0的day分区，后面又出现了数据不为0的day分区，需要初始化最早断流日期
                        first_cut_off_day = '2100-01-01'
                    # 断流场景
                    elif int(size) == 0 and update_date <= day:
                        if update_date < first_cut_off_day:
                            print 'new first_cut_off_day:', update_date
                            first_cut_off_day = update_date
                            first_cut_off_hdfs_path = full_path.replace('hdfs://sg-nn2.bigdata.bigo.inner:8888', '')
                            diff = (parse_ymd(day) - parse_ymd(update_date)).days
                            if diff >= 100:
                                abnormal_info = 'CutOff100+Days'
                            else:
                                abnormal_info = 'CutOff%iDays' % (diff+1)
                    # 普通场景，统计当日数据量
                    elif int(size) != 0 and update_date == day:
                        table_size_new += int(size)
                        file_size += int(size)
                        abnormal_info = 'normal'
                        hdfs_path = full_path.replace('hdfs://sg-nn2.bigdata.bigo.inner:8888', '')
                        break
            # 循环结束时，size都不为0，但是最后一条数据的update_date仍小于day时，属于调度停更，需要更新异常
            if file_size != 0 and update_date < day and abnormal_info == 'normal':
                diff = (parse_ymd(day) - parse_ymd(update_date)).days
                if diff >= 100:
                    abnormal_info = 'TaskStop100+Days'
                else:
                    abnormal_info = 'TaskStop%iDays' % (diff + 1)
            # 如果当日数据不为0，即使前面有断流，也算作正常
            elif file_size != 0:
                abnormal_info = 'normal'
            # 当日数据为0，
            elif file_size == 0:
                hdfs_path = first_cut_off_hdfs_path
        else:
            # du为空的场景比较少见，仅限于通用打点事件不存在
            abnormal_info = 'PartitionNotExists'
    table_size = table_size_new if tb_source == 'common_event' else table_size
    print 'count_hdfs_size_end:', [tb_name, event_id, tb_source, hdfs_path, file_size, abnormal_info,table_size,query_times,table_partition, day]
    return [tb_name, event_id, tb_source, hdfs_path, long(file_size), abnormal_info,long(table_size),query_times,table_partition, day]


def count_hdfs_size_hour(info_list):
    print 'count_hdfs_size_hour_begin:', info_list
    tb_name, event_id, tb_source, hdfs_path, abnormal_info, table_size, query_times, table_partition, day, hour = info_list
    file_size, update_date = 0, ''
    # 正常表需要统计大小，任务停更的表需要确认是否真实停更，空表错表直接返回
    if abnormal_info == 'normal' or ('TaskStop' in abnormal_info and int(re.findall(r"\d+",abnormal_info)[0]) < 100):
        if tb_source == 'common_event':
            # 通用打点表
            ls_cmd = "hadoop fs -ls hdfs://sg-nn2.bigdata.bigo.inner:8888%s/day=%s/hour=%s/event_id=%s|awk -F' ' '{print $5,$6,$7,$8}'" % (hdfs_path, day, hour, event_id)
        else:
            if 'hour' in table_partition:
                # 普通打点表
                ls_cmd = "hadoop fs -ls hdfs://sg-nn2.bigdata.bigo.inner:8888%s/day=%s/hour=%s|awk -F' ' '{print $5,$6,$7,$8}'" % (hdfs_path, day, hour)
            else:
                ls_cmd = "hadoop fs -ls hdfs://sg-nn2.bigdata.bigo.inner:8888%s/day=%s|awk -F' ' '{print $5,$6,$7,$8}'" % (hdfs_path, day)
        with os.popen(ls_cmd, 'r') as p:
            stdout = p.read().strip()
        # 为空或者-1时，数据为空，需要拼接hdfs路径，后续正常运算即可
        if stdout == '':
            if 'day' in table_partition:
                hdfs_path += '/day=%s' % day
            if 'hour' in table_partition:
                hdfs_path += '/hour=%s' % hour
            if 'event_id' in table_partition:
                hdfs_path += '/event_id=%s' % event_id
        # ls后的数据处理流程，精确统计分区里的文件大小
        else:
            info_list = stdout.split('\n')
            have_day_partition, have_hour_partition, have_event_partition = 'day' in table_partition, 'hour' in table_partition, 'event_id' in table_partition
            for info in info_list:
                if len(info) > 3 and '.inprogress.' not in info:
                    size, update_date, update_time, full_path = info.split(' ')
                    # 普通打点表有天分区也有小时分区，有天分区没有小时分区时需要过滤，有小时分区时可直接累加
                    # 通用打点表有小时分区和事件分区，可以直接进行累加
                    if tb_source == 'normal_event' and have_day_partition and not have_hour_partition:
                        if update_time.split(':')[0] == hour:
                            file_size += long(size)
                    else:
                        file_size += long(size)
            # 构造待检查的hdfs详细路径
            if have_day_partition and have_hour_partition and have_event_partition:
                hdfs_path = hdfs_path + '/day=' + day + '/hour=' + hour + '/event_id=' + event_id
            elif have_day_partition and have_hour_partition:
                hdfs_path = hdfs_path + '/day=' + day + '/hour=' + hour
            elif have_day_partition:
                hdfs_path = hdfs_path + '/day=' + day
            else:
                pass
            if file_size > 0:
                abnormal_info = 'normal'
    print 'count_hdfs_size_hour_end:', [tb_name, event_id, tb_source, hdfs_path, long(file_size), abnormal_info,long(table_size),query_times,table_partition, day,hour]
    return [tb_name, event_id, tb_source, hdfs_path, long(file_size), abnormal_info,long(table_size),query_times,table_partition, day, hour]


if __name__ == '__main__':
    # 导表全量表每天计算一次，待检查的天分区为前一天
    # 其他来源表都是每小时计算一次，待检查的天分区为当天
    day = sys.argv[1]
    # 导表全量表输入的小时为 --
    hour = sys.argv[2]
    sql_str = "select * from dwd.get_table_hdfs_path_di where hdfs_path!='' "
    if hour != '--':
        sql_str += "and tb_source!='bdp'"
    sparkSession = SparkSession.builder.appName('get_table_hdfs_path_di').enableHiveSupport().getOrCreate()
    hive_ret = sparkSession.sql(sql_str)
    ret = hive_ret.collect()
    table_info, imo_us_info = list(), dict()
    for r in ret:
        if r.tb_source != 'imo_us':
            table_info.append([r.tb_name, r.event_id, r.tb_source, r.hdfs_path, r.abnormal_info, r.table_size, r.query_times, r.extra, day, hour])
        else:
            imo_us_info[r.tb_name] = (r.table_size, r.query_times)
    print 'table_info len is %i\n table_info:\n' % len(table_info), table_info
    print 'imo_us_info len is %i\n imo_us_info:\n' % len(imo_us_info), imo_us_info
    pool = multiprocessing.Pool(processes=10)
    if hour == '--':
        columns = ['tb_name', 'event_id', 'tb_source', 'hdfs_path','file_size', 'abnormal_info', 'table_size', 'query_times',
                   'table_partition', 'day']
        pool_outputs = pool.map(count_hdfs_size_day, table_info)
        insert_tb = 'dwd.count_hdfs_size_di'
        res = count_imo_us_size_day(day, imo_us_info)
        pool_outputs.extend(res)
    else:
        columns = ['tb_name', 'event_id', 'tb_source', 'hdfs_path', 'file_size', 'abnormal_info', 'table_size',
                   'query_times','table_partition', 'day','hour']
        pool_outputs = pool.map(count_hdfs_size_hour, table_info)
        insert_tb = 'dwd.count_hdfs_size_hi'
        res = count_imo_us_size_hour(day, hour, imo_us_info)
        pool_outputs.extend(res)
    pool.close()
    pool.join()
    print 'pool_outputs len is %i\n pool_outputs:' % len(pool_outputs), pool_outputs
    pd_df = pd.DataFrame(pool_outputs, columns=columns)
    spark_df = sparkSession.createDataFrame(pd_df)
    spark_df.createOrReplaceTempView('count_hdfs_size_view')
    insert_sql = "insert overwrite table %s select * from count_hdfs_size_view" % insert_tb
    sparkSession.sql(insert_sql)
    if hour == '--':
        sql_cut0ff = """
            insert overwrite table dwd.cut_off_table_di partition(day='%s')
            select
            if(tb_source='common_event', concat(tb_name,'+',event_id), tb_name) as tb_name
            ,tb_source
            ,abnormal_info
            ,table_size
            ,query_times
            ,extra
            from dwd.count_hdfs_size_di
            where day='%s' and abnormal_info!='normal'
        """ % (day, day)
        print 'insert sql_cut0ff:\n%s' % sql_cut0ff
        sparkSession_cutoff = SparkSession.builder.appName('cut_off_table_di').enableHiveSupport().getOrCreate()
        sparkSession_cutoff.sql(sql_cut0ff)
