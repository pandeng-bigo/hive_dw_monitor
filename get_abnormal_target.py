# coding: utf-8
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql import SparkSession
import numpy as np
from scipy import stats
from datetime import datetime,timedelta
import requests
import json
import pandas as pd
import time
import re

"""
create table if not exists dwd.get_abnormal_target_di
(
    app_name string comment '业务名'
    ,table_name string comment '表名'
    ,table_owner string comment '表归属人'
    ,table_size bigint comment '表大小'
    ,query_times int comment '表昨日访问次数'
    ,downstream_user string comment '下游业务用户,用逗号分隔'
    ,update_time bigint comment '维度-指标更新时间'
    ,dimension string comment '维度信息,多个维度用逗号分隔,维度名与值之间用冒号分隔'
    ,target_name string comment '指标名称'
    ,target_type string comment '指标类型,包括id类,核心维度类,数值类,属性类'
    ,target_value double comment '指标取值'
    ,row_num bigint comment '该维度行数'
) comment '指标异常信息天表'
partitioned by
(
    day string comment '天分区'
    ,task_flag string comment 'task标识,取值有early和else'
)
stored as orc
tblproperties (
    'orc.compress'='snappy'
);
create table if not exists dwd.monitor_column_whitelist_all(
 table_name string COMMENT '表名',
 target_name string COMMENT '字段名'
 )
COMMENT '指标监控白名单-人工剔除不需要监控的表字段'
ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ','
 ESCAPED BY '\\'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE ;
"""
def wechat_robot(message, keys, msgtype, user):
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
        if min(to_compare) >= 0:
            too_big = 0 if np.isnan(too_big) or too_big <= 0 else too_big
            too_small = 0 if np.isnan(too_small) or too_small <= 0 else too_small
        else:
            too_big = 0 if np.isnan(too_big) else too_big
            too_small = 0 if np.isnan(too_small) else too_small
        # print 'too_big:%s, too_small:%s' % (too_big, too_small)
        return round(too_big, 2), round(too_small, 2)
    elif len(to_compare) == 1:
        return round(to_compare[0] * 2.0, 2), round(to_compare[0] * 0.3, 2)
    else:
        return 0, 0


def linear_regression_check(to_compare, k=0.7):
    if len(to_compare) > 1:
        slope, intercept, r_value, p_value, std_err = stats.linregress([i for i in range(len(to_compare))], to_compare)
        # 计算期线性回归期望值
        mean = len(to_compare) * slope + intercept
        too_big = mean * (1.3 + k)
        too_small = mean * (1 - k)
        if min(to_compare) >= 0:
            too_big = 0 if np.isnan(too_big) or too_big <= 0 else too_big
            too_small = 0 if np.isnan(too_small) or too_small <= 0 else too_small
        else:
            too_big = 0 if np.isnan(too_big) else too_big
            too_small = 0 if np.isnan(too_small) else too_small
        return round(too_big, 2), round(too_small, 2)
    elif len(to_compare) == 1:
        return round(to_compare[0] * (1.3 + k), 2), round(to_compare[0] * (1 - k), 2)
    else:
        return 0, 0


if __name__ == '__main__':
    # 获取当前day
    day = sys.argv[1]
    task_flag = sys.argv[2]
    if task_flag == 'early':
        condition = '<'
    else:
        condition = '>='
    sql_str = """
                select
                t1.app_name
                ,t1.table_name
                ,t1.dimension
                ,t1.target_name
                ,t1.target_type
                ,t1.target_value
                ,t1.day
                ,t3.table_owner,t3.table_size,t3.query_times,t3.downstream_user
                ,unix_timestamp(concat(t1.day,' 00:00:00')) as update_time
                ,t5.total_value
                ,t6.row_num
                ,t6.total_row
                from dwd.monitor_column_target_info_di t1 
                join 
                (
                    select table_name from dwd.monitor_column_target_info_di where day='%s' and task_id %s 1000 group by table_name
                ) t0 on t1.table_name=t0.table_name
                left join 
                (
                    select table_name,dimension
                    from dwd.monitor_column_target_info_di 
                    where day between date_sub('%s', 45) and '%s' and target_name='row_num' 
                    and day_size/target_value>100*1024 -- 1行数据大于100k
                    and target_value<1000              -- 且行数在1000以内
                    group by table_name,dimension
                    union all 
                    select table_name,dimension
                    from dwd.monitor_column_target_info_di 
                    where day between date_sub('%s', 45) and '%s'
                    group by table_name,dimension
                    having count(distinct day)<5     -- 表纳入监控前至少已经有5天的数据
                ) t2 on t1.table_name=t2.table_name and t1.dimension=t2.dimension
                left join 
                (
                    select table_name,table_owner,table_size,query_times,downstream_user
                    from dwd.monitor_column_target_info_di 
                    where day='%s'
                    group by table_name,table_owner,table_size,query_times,downstream_user
                ) t3 on t1.table_name=t3.table_name
                left join dwd.monitor_column_whitelist_all t4 on t1.table_name=t4.table_name or regexp_replace(t1.target_name,'_sum_poly1|_avg_poly1|_num_poly2|_rate_poly2|_num_poly3|_rate_poly3','')=t4.target_name
                left join
                (
                    select day,table_name,target_name,sum(target_value) as total_value
                    from dwd.monitor_column_target_info_di
                    where day between date_sub('%s', 45) and '%s'
                    group by day,table_name,target_name
                ) t5 on t1.day=t5.day and t1.table_name=t5.table_name and t1.target_name=t5.target_name
                left join 
                (
                    select day,table_name,dimension,target_value as row_num,sum(target_value) over(partition by day,table_name) as total_row
                    from dwd.monitor_column_target_info_di 
                    where day between date_sub('%s', 45) and '%s' and target_name='row_num' 
                ) t6 on t1.day=t6.day and t1.table_name=t6.table_name and t1.dimension=t6.dimension
                where t1.day between date_sub('%s', 45) and '%s' and lower(t1.dimension) not regexp 'tiki'
                and t2.table_name is null
                and t4.table_name is null
                order by t1.day
                """ % (day, condition,day, day, day,day, day,day,day,day,day,day,day)
    print sql_str
    sparkSession = SparkSession.builder.appName('get_monitor_column_target_info_di').enableHiveSupport().getOrCreate()
    hive_ret = sparkSession.sql(sql_str)
    ret = hive_ret.collect()
    current, to_compare = {}, {}
    for r in ret:
        app_name = r.app_name
        table_name = r.table_name
        table_owner = r.table_owner
        table_size = r.table_size
        query_times = r.query_times
        downstream_user = r.downstream_user
        dimension = r.dimension
        target_name = r.target_name
        target_type = r.target_type
        target_value = r.target_value
        update_time = r.update_time
        total_value = r.total_value
        row_num = r.row_num
        total_row = r.total_row
        # 计算每天每个指标的数值权重和行数权重
        weight_value = abs(round(target_value * 100.0 / total_value, 2)) if total_value != 0 else 0
        weight_row = abs(round(row_num * 100.0 / total_row, 2)) if total_row != 0 else 0
        if re.search('re[1-9]|re_[1-9]|[1-9]re|rr[1-9]|mr[1-9]|afr[1-9]|nfr[1-9]|retian[1-9]|remain_[1-9]', target_name):
            day_num = int(re.findall(r"\d+", target_name)[0])
        else:
            day_num = 0
        day_str = (parse_ymd(day) - timedelta(day_num)).strftime('%Y-%m-%d')
        # 留存N日,往前推N日数据当做当日数据去判断
        if (r.day == day and not re.search('re[1-9]|re_[1-9]|[1-9]re|rr[1-9]|mr[1-9]|afr[1-9]|nfr[1-9]|retian[1-9]|remain_[1-9]', target_name)) \
                or (r.day == day_str and re.search('re[1-9]|re_[1-9]|[1-9]re|rr[1-9]|mr[1-9]|afr[1-9]|nfr[1-9]|retian[1-9]|remain_[1-9]', target_name)):
            if (app_name, table_name,table_owner,table_size,query_times,downstream_user,dimension,target_name,target_type) not in current:
                current[(app_name, table_name,table_owner,table_size,query_times,downstream_user,dimension,target_name,target_type)] = (target_value,weight_value,weight_row,row_num,update_time)
        elif (r.day < day and not re.search('re[1-9]|re_[1-9]|[1-9]re|rr[1-9]|mr[1-9]|afr[1-9]|nfr[1-9]|retian[1-9]|remain_[1-9]', target_name)) \
                or (r.day < day_str and re.search('re[1-9]|re_[1-9]|[1-9]re|rr[1-9]|mr[1-9]|afr[1-9]|nfr[1-9]|retian[1-9]|remain_[1-9]', target_name)):
            if (app_name, table_name,table_owner,table_size,query_times,downstream_user,dimension,target_name,target_type) not in to_compare:
                to_compare[(app_name, table_name,table_owner,table_size,query_times,downstream_user,dimension,target_name,target_type)] = []
            to_compare[(app_name, table_name,table_owner,table_size,query_times,downstream_user,dimension,target_name,target_type)].append((target_value,weight_value,weight_row,row_num,update_time))
        else:
            pass
    err_info = []
    wechat_dict = dict()
    null_table, null_dimension, new_dimension = dict(), dict(), dict()
    for k in sorted(current.keys()):
        app_name, table_name, table_owner, table_size, query_times, downstream_user, dimension, target_name, target_type = k
        target_value, weight_value_today, weight_row_today, row_num, update_time = current[k]
        # 当前数值权重大于3%的指标才有价值告警
        # 默认值占比小于5的指标无告警价值
        if weight_value_today > 3 and not ('rate_poly3' in target_name and target_value<5) and not ('num_poly3' in target_name and target_value<1000):
            if k in to_compare:
                # 获取历史权重中位数
                weight_value_his = round(np.median([c[1] for c in to_compare[k]]),2)
                weight_row_his = round(np.median([c[2] for c in to_compare[k]]),2)
                # 正态分布区间
                too_big, too_small = std_check([c[0] for c in to_compare[k]])
                # 线性区间
                too_big2, too_small2 = linear_regression_check([c[0] for c in to_compare[k]])
                # 取两者的并集区间，不满足时才触发告警
                too_big = too_big2 if too_big2 > too_big else too_big
                too_small = too_small2 if too_small2 < too_small else too_small
                mean = (too_small + too_big) / 2
                # # 监控rate类数据时，gap大于70时才告警
                # rate_flag = abs(current[k] - mean) >= 70
                # 监控非rate类数据时，gap大于1000时才告警,以排除小维度数据
                # num_flag = abs(current[k] - mean) >= 1000
                # 不在置信区间，或者有效值占比小于50%
                if target_value < too_small or target_value > too_big:
                    print 'abnormal:', too_small, target_value, too_big, table_name, dimension, target_name
                    print [c[0] for c in to_compare[k]]
                    err_info.append([app_name, table_name, table_owner, table_size, query_times, downstream_user, update_time, dimension, target_name, target_type, target_value])
                    if (app_name, table_name, table_owner, table_size, query_times, downstream_user) not in wechat_dict:
                        wechat_dict[(app_name, table_name, table_owner, table_size, query_times, downstream_user)] = []
                    wechat_dict[(app_name, table_name, table_owner, table_size, query_times, downstream_user)].append([dimension, target_name, target_type, target_value, weight_value_his, weight_row_his,weight_value_today, weight_row_today, too_big, too_small, mean, row_num])
                    for compare_value in to_compare[k]:
                        target_value, weight_value, weight_row, row_num, update_time = compare_value
                        err_info.append([app_name, table_name, table_owner, table_size, query_times, downstream_user, update_time, dimension, target_name, target_type, target_value])
            # 新产生的维度数据
            else:
                if target_value != 0:
                    print 'new:', table_name, dimension, target_name, dimension, target_value
                    err_info.append([app_name, table_name, table_owner, table_size, query_times, downstream_user, update_time, dimension,target_name, target_type, target_value])
                    if table_name not in new_dimension:
                        new_dimension[table_name] = [set(), set()]
                    new_dimension[table_name][0].add(dimension)
                    new_dimension[table_name][1].add(re.sub('_sum_poly1|_avg_poly1|_num_poly2|_rate_poly2|_num_poly3|_rate_poly3', '', target_name))
                    for i in range(30):
                        update_time_tmp = update_time - 24*3600*(i+1)
                        err_info.append([app_name, table_name, table_owner, table_size, query_times, downstream_user, update_time_tmp,dimension, target_name, target_type, 0])
    # 当日数据缺少的维度
    for k in sorted(to_compare.keys()):
        app_name, table_name, table_owner, table_size, query_times, downstream_user, dimension, target_name, target_type = k
        # 正态分布区间
        too_big, too_small = std_check([c[0] for c in to_compare[k]])
        # 线性区间
        too_big2, too_small2 = linear_regression_check([c[0] for c in to_compare[k]])
        # 取两者的并集区间，不满足时才触发告警
        too_big = too_big2 if too_big2 > too_big else too_big
        too_small = too_small2 if too_small2 < too_small else too_small
        mean = (too_small + too_big) / 2
        if k not in current and mean != 0 and not re.search('re[1-9]|re_[1-9]|[1-9]re|rr[1-9]|mr[1-9]|afr[1-9]|nfr[1-9]|retian[1-9]|remain_[1-9]', target_name):
            print 'miss:', table_name, dimension, target_name, mean
            if dimension == '':
                if table_name not in null_table:
                    null_table[table_name] = set()
                null_table[table_name].add(re.sub('_sum_poly1|_avg_poly1|_num_poly2|_rate_poly2|_num_poly3|_rate_poly3', '', target_name))
            elif not re.search('其他|其它|other|unknown', dimension.lower()):
                if table_name not in null_dimension:
                    null_dimension[table_name] = [set(), set()]
                null_dimension[table_name][0].add(dimension)
                null_dimension[table_name][1].add(re.sub('_sum_poly1|_avg_poly1|_num_poly2|_rate_poly2|_num_poly3|_rate_poly3', '', target_name))
            for compare_value in to_compare[k]:
                target_value, weight_value, weight_row, row_num, update_time = compare_value
                err_info.append([app_name, table_name, table_owner, table_size, query_times, downstream_user, update_time,dimension, target_name, target_type, target_value])
            update_time_today = int(time.mktime(time.strptime(day, '%Y-%m-%d')))
            err_info.append([app_name, table_name, table_owner, table_size, query_times, downstream_user, update_time_today, dimension,target_name, target_type, 0])
    if len(err_info) > 0:
        sparkSession = SparkSession.builder.appName('insert_get_abnormal_target_di').enableHiveSupport().getOrCreate()
        pd_df = pd.DataFrame(err_info,columns=['app_name', 'table_name', 'table_owner', 'table_size', 'query_times', 'downstream_user','update_time', 'dimension', 'target_name','target_type', 'target_value'])
        spark_df = sparkSession.createDataFrame(pd_df)
        spark_df.createOrReplaceTempView('get_abnormal_target_di_view')
        sql_insert = """
        insert overwrite table dwd.get_abnormal_target_di partition(day='%s',task_flag='%s')
        select
        t1.*,coalesce(t2.row_num, 0) as row_num
        from 
        get_abnormal_target_di_view t1
        left join 
        (
            select table_name,dimension,target_value as row_num
            from dwd.monitor_column_target_info_di 
            where day='%s' and target_name='row_num' 
            group by table_name,dimension,target_value
        ) t2 on t1.table_name=t2.table_name and t1.dimension=t2.dimension
        """ % (day,task_flag,day)
        sparkSession.sql(sql_insert)
        # 每天发送2次汇总信息到企业微信群
        msg = "# <font color=\"info\">指标监控-天级(%s-%s):</font>\n" % (day, task_flag)
        index = 1
        for k in sorted(wechat_dict.keys(), key=lambda k:(k[0] , k[1]), reverse=True):
            app_name, table_name, table_owner, table_size, query_times, downstream_user = k
            downstream_user = ','.join(downstream_user.split(',')[:3])  # 只取前三个下游用户
            # 数值处理为字符串
            if long(table_size) >= 1024 ** 4:
                table_size_str = '%0.2fTb' % (long(table_size) * 1.0 / 1024 ** 4)
            elif long(table_size) >= 1024 ** 3:
                table_size_str = '%0.2fGb' % (long(table_size) * 1.0 / 1024 ** 3)
            elif long(table_size) >= 1024 ** 2:
                table_size_str = '%0.2fMb' % (long(table_size) * 1.0 / 1024 ** 2)
            else:
                table_size_str = '%0.2fKb' % (long(table_size) * 1.0 / 1024)
            if downstream_user == '':
                downstream_user_str = ''
            else:
                downstream_user_str = '-下游用户:%s' % downstream_user
            msg += "%i.<font color=\"warning\">%s<@%s>(表大小:%s-查询次数:%s%s)</font>\n" % (index, table_name, table_owner, table_size_str, query_times, downstream_user_str)
            target_index = 0
            for r in wechat_dict[k]:
                dimension, target_name, target_type, target_value, weight_value_his, weight_row_his, weight_value_today, weight_row_today, too_big, too_small, mean, row_num = list(r)
                if mean != 0:
                    rate = round(target_value * 1.0 / mean, 2)
                    rate_str = '是期望值的<font color=\"warning\">%s</font>倍' % rate
                else:
                    rate_str = '历史值都为0'
                if dimension == '':
                    dimension_str = ''
                    if target_index == 0:
                        weight_str = '表行数:%i\n' % row_num
                    else:
                        weight_str = ''
                else:
                    dimension_str = '%s\n' % dimension
                    weight_str = '当前维度行数:%i(行数权重:%s%%),当前维度指标权重:%s%%(历史权重:%s%%)\n' % (row_num, weight_row_today, weight_value_today, weight_value_his)
                if target_value >= 100000000:
                    target_num = '%0.2f亿' % (target_value / 100000000.0)
                    too_small_num = '%0.2f亿' % (too_small / 100000000.0) if int(too_small) != 0 else '0'
                    too_big_num = '%0.2f亿' % (too_big / 100000000.0)
                elif target_value >= 10000:
                    target_num = '%0.2f万' % (target_value / 10000.0)
                    too_small_num = '%0.2f万' % (too_small / 10000.0) if int(too_small) != 0 else '0'
                    too_big_num = '%0.2f万' % (too_big / 10000.0)
                else:
                    target_num = '%0.2f' % target_value
                    too_small_num = '%0.2f' % too_small if int(too_small) != 0 else '0'
                    too_big_num = '%0.2f' % too_big
                    # 如果保留两位小数的结果仍是整数，只保留整数部分
                    target_num = target_num.split('.')[0] if target_num.endswith('.00') else target_num
                    too_small_num = too_small_num.split('.')[0] if too_small_num.endswith('.00') else too_small_num
                    too_big_num = too_big_num.split('.')[0] if too_big_num.endswith('.00') else too_big_num
                if 'rate_poly' not in target_name:
                    interval_str = '%s not in [%s,%s],%s' % (target_num, too_small_num, too_big_num, rate_str)
                else:
                    interval_str = '%s%% not in [%s%%,%s%%],%s' % (target_value, too_small, too_big, rate_str)
                if '_sum_poly1' in target_name:
                    target_name_str = '%s(指标和)' % target_name.replace('_sum_poly1', '')
                elif '_avg_poly1' in target_name:
                    target_name_str = '%s(有效值均值)' % target_name.replace('_avg_poly1', '')
                elif '_num_poly2' in target_name:
                    target_name_str = '%s(有效值个数)' % target_name.replace('_num_poly2', '')
                elif '_rate_poly2' in target_name:
                    target_name_str = '%s(有效值占比)' % target_name.replace('_rate_poly2', '')
                elif '_num_poly3' in target_name:
                    target_name_str = '%s(默认值个数)' % target_name.replace('_num_poly3', '')
                elif '_rate_poly3' in target_name:
                    target_name_str = '%s(默认值占比)' % target_name.replace('_rate_poly3', '')
                elif 'row_num' == target_name:
                    target_name_str = '%s(行数)' % target_name
                else:
                    target_name_str = '监控策略外,请修正'
                msg += "<font color=\"comment\">%s%s%s:%s</font>\n-----------\n" % (dimension_str,weight_str, target_name_str,interval_str)
                if len(msg.encode('utf-8')) > 3000:
                    if msg[-12:] == '-----------\n':
                        msg = msg[:-12]
                    wechat_robot(msg, '015e94be-ad50-4d07-a231-72b8463b1b47', 'markdown', '')
                    print 'msg长度为:%i\n' % len(msg), msg
                    msg = ''
                target_index += 1
            index += 1
            if msg[-12:] == '-----------\n':
                msg = msg[:-12]
        if len(null_table) > 0:
            for table_name, target_name_set in null_table.items():
                index += 1
                msg += '%i.<font color=\"warning\">表%s缺失的指标:</font>\n' % (index,table_name)
                tmp = ''
                for target in target_name_set:
                    tmp += '%s,' % target
                msg += '<font color=\"comment\">%s</font>\n' % tmp[:-1]
                if len(msg.encode('utf-8')) > 3000:
                    wechat_robot(msg, '015e94be-ad50-4d07-a231-72b8463b1b47', 'markdown', '')
                    print 'msg长度为:%i\n' % len(msg), msg
                    msg = ''
                msg += '-----------\n'
            if msg[-12:] == '-----------\n':
                msg = msg[:-12]
        if len(null_dimension) > 0:
            for k, v in null_dimension.items():
                index += 1
                table_name = k
                msg += '%i.<font color=\"warning\">表%s缺失的维度指标:</font>\n' % (index,table_name)
                tmp = ''
                for dimension in v[0]:
                    tmp += '%s,' % dimension
                msg += '<font color=\"comment\">%s</font>\n' % tmp[:-1]
                if len(msg.encode('utf-8')) > 3000:
                    wechat_robot(msg, '015e94be-ad50-4d07-a231-72b8463b1b47', 'markdown', '')
                    print 'msg长度为:%i\n' % len(msg), msg
                    msg = ''
                msg += '-----------\n'
                tmp = ''
                for target in v[1]:
                    tmp += '%s,' % target
                msg += '<font color=\"comment\">%s</font>\n' % tmp[:-1]
                if len(msg.encode('utf-8')) > 3000:
                    wechat_robot(msg, '015e94be-ad50-4d07-a231-72b8463b1b47', 'markdown', '')
                    print 'msg长度为:%i\n' % len(msg), msg
                    msg = ''
        if len(new_dimension) > 0:
            for k, v in new_dimension.items():
                index += 1
                table_name = k
                msg += '%i.<font color=\"warning\">表%s新增的维度指标:</font>\n' % (index,table_name)
                tmp = ''
                for dimension in v[0]:
                    tmp += '%s,' % dimension
                msg += '<font color=\"comment\">%s</font>\n' % tmp[:-1]
                if len(msg.encode('utf-8')) > 3000:
                    wechat_robot(msg, '015e94be-ad50-4d07-a231-72b8463b1b47', 'markdown', '')
                    print 'msg长度为:%i\n' % len(msg), msg
                    msg = ''
                msg += '-----------\n'
                tmp = ''
                for target in v[1]:
                    tmp += '%s,' % target
                msg += '<font color=\"comment\">%s</font>\n' % tmp[:-1]
                if len(msg.encode('utf-8')) > 3000:
                    wechat_robot(msg, '015e94be-ad50-4d07-a231-72b8463b1b47', 'markdown', '')
                    print 'msg长度为:%i\n' % len(msg), msg
                    msg = ''
        msg += "\n## 指标值波动汇总见[grafana](http://metrics-sysop.bigo.sg/d/cmBAXZfVz/zhi-biao-jian-kong?orgId=1&from=now-7d&to=now&var-day=%s)" % day
        print 'msg长度为:%i\n' % len(msg), msg
        wechat_robot(msg, '015e94be-ad50-4d07-a231-72b8463b1b47', 'markdown', '')
    else:
        msg = "# <font color=\"info\">指标监控-天级(%s-%s):指标正常</font>\n" % (day, task_flag)
        wechat_robot(msg, '015e94be-ad50-4d07-a231-72b8463b1b47', 'markdown', '')