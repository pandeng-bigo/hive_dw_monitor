# coding: utf-8
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql import SparkSession
import time
import re
import pandas as pd


def create_table_sql(day):
    """
    create table if not exists dwd.monitor_column_table_sql_di
    (
      app_name string comment '业务名'
      ,table_name string comment '表名'
      ,table_owner string comment '表归属人'
      ,table_partition string comment '表分区信息,多个分区用逗号分隔'
      ,table_size bigint comment '表大小'
      ,table_update_time string comment '表更新的时间'
      ,query_times int comment '表昨日访问次数'
      ,day_size bigint comment '最近10日数据量均值,作为后续拼接sql的参考'
      ,downstream_user string comment '下游业务用户,用逗号分隔,只对数仓业务线有价值'
      ,uid_column string comment 'uid类型的字段,用逗号分隔'
      ,string_id_column string comment '字符串id类型的字段,用逗号分隔'
      ,num_id_column string comment '数字id类型的字段,用逗号分隔'
      ,main_dimension_column string comment '主要维度类型的字段,用逗号分隔'
      ,not_monitor_column string comment '不监控的字段,用逗号分隔'
      ,values_column string comment '指标类型的字段,用逗号分隔'
      ,attributes_column string comment '属性类型的字段,用逗号分隔'
      ,format_sql string comment '对单表进行统计的sql'
    ) comment '生成表的统计sql,用于后续拼接为task来运行'
    partitioned by
    (
        day string comment '天分区'
    )
    stored as orc
    tblproperties (
      'orc.compress'='snappy'
    );
    """

    sql_str = """
            select 
            app_name
            ,table_name
            ,table_owner
            ,table_partition
            ,table_size
            ,table_status as table_update_time
            ,query_times
            ,day_size
            ,downstream_user
            ,CONCAT_WS('--',collect_set(concat(column_name,'||',column_type,'||',column_comment))) as columns_info
            from dwd.monitor_column_info_di
            where day='%s'
            group by 
            app_name
            ,table_name
            ,table_owner
            ,table_partition
            ,table_size
            ,table_status
            ,query_times
            ,day_size
            ,downstream_user
    """ % day
    sparkSession = SparkSession.builder.appName('get_monitor_column_info_di').enableHiveSupport().getOrCreate()
    hive_ret = sparkSession.sql(sql_str)
    ret = hive_ret.collect()
    data_list = []
    for r in ret:
        app_name = r.app_name
        table_name = r.table_name
        table_owner = r.table_owner
        table_partition = r.table_partition
        table_size = r.table_size
        table_update_time = r.table_update_time
        query_times = r.query_times
        downstream_user = r.downstream_user
        day_size = r.day_size
        columns_info = r.columns_info
        column_dict = {
            'uid': []
            ,'string_id': []
            ,'num_id': []
            ,'main_dimension': []
            ,'not_monitor': []
            ,'values': []
            ,'attributes': []
        }
        columns_list = columns_info.split('--')
        uid_str, num_id_str,string_id_str, main_dimension_str, attributes_str,values_str  = '', '', '', '', '', ''
        another_name_list = []
        for column in columns_list:
            # 加策略后缀
            column_name, column_type, column_comment = column.split('||')
            # 日期类，补充类，复杂结构类字段不监控
            if (column_name.endswith('day') and column_type=='string') \
                or column_name in ('days', 'time') \
                or re.search('timestamp|start_time|starttime|create_time|end_time|enter_time|exit_time|update_time|rtime|ctime'
                             '|update_rtime|date_time|datetime|time_zone|event_time|param|reserve', column_name) \
                or re.search('array|map|struct', column_type):
                column_dict['not_monitor'].append(column_name)
            # 业务表才有核心维度概念，数仓表都当做属性字段处理
            elif app_name!='dw' and column_name in ('os', 'fin_region', 'region', 'area','op_area', 'app','app_name') and column_type == 'string':
                column_dict['main_dimension'].append(column_name)
                main_dimension_str += '{column_name},\n'.format(column_name=column_name)
            # 区分uid与普通id，处理策略相同，但是口径不同
            elif column_name in ('user_id','owner_id','author_id','creator','uid64') or (column_name.endswith('uid') and column_name not in ('guid')):
                column_dict['uid'].append('%s_num_poly2' % column_name)
                column_dict['uid'].append('%s_rate_poly2' % column_name)
                another_name_list.append('%s_num_poly2' % column_name)
                another_name_list.append('%s_rate_poly2' % column_name)
                uid_str += 'count(if(coalesce(cast({column_name} as bigint), 0)>0,1,null)) as {column_name}_num_poly2,\n' \
                           'round(count(if(coalesce(cast({column_name} as bigint), 0)>0,1,null))*100.0/count(1),2) as {column_name}_rate_poly2,\n'.format(column_name=column_name)
            elif column_name.endswith('id'):
                another_name_list.append('%s_num_poly3' % column_name)
                another_name_list.append('%s_rate_poly3' % column_name)
                if column_type == 'string':
                    column_dict['string_id'].append('%s_num_poly3' % column_name)
                    column_dict['string_id'].append('%s_rate_poly3' % column_name)
                    string_id_str += "count(if({column_name} is null or {column_name}='' or lower({column_name})='unknown',1,null)) as {column_name}_num_poly3,\n" \
                                     "round(count(if({column_name} is null or {column_name}='' or lower({column_name})='unknown',1,null))*100.0/count(1),2) as {column_name}_rate_poly3,\n".format(column_name=column_name)
                else:
                    column_dict['num_id'].append('%s_num_poly3' % column_name)
                    column_dict['num_id'].append('%s_rate_poly3' % column_name)
                    num_id_str += "count(if({column_name} is null or {column_name}<=0,1,null)) as {column_name}_num_poly3,\n" \
                                  "round(count(if({column_name} is null or {column_name}<=0,1,null))*100.0/count(1),2) as {column_name}_rate_poly3,\n".format(column_name=column_name)
            # 数值指标类字段需要重点监控
            # 拆分优化下
            elif (re.search('dau|mau|pv|uv|pcu|pcr|acu|arpu|arppu|re[1-9]|re_[1-9]|[1-9]re|rr[1-9]|mr[1-9]|afr[1-9]|nfr[1-9]|retian[1-9]|remain_[1-9]|amount|amt|cnt|num|sum|avg|count|rate|ratio|percentage'
                            '|users|people|fans|orders|_order|rooms|change|pay|commission|price|prize|cost|recharge|revenue|premium|_goal'
                            '|income|dollar|money|diamond|bean|balance|ticket|duration|stay_time|watch|_day|_0[1-9]|d[0-9]|day[0-9]'
                            '|data[0-9]|_vv|official_all' ,column_name)
                  or column_name.endswith('stay_time') or column_name.endswith('staytime') or column_name.endswith('times')
                  ) and column_type in ('float','int','bigint','double'):
                column_dict['values'].append('%s_sum_poly1' % column_name)
                column_dict['values'].append('%s_avg_poly1' % column_name)
                another_name_list.append('%s_sum_poly1' % column_name)
                another_name_list.append('%s_avg_poly1' % column_name)
                values_str += "coalesce(sum({column_name}),0) as {column_name}_sum_poly1,\n" \
                              "coalesce(round(sum({column_name})*1.0/count(if({column_name}!=0,1,null)),2),0) as {column_name}_avg_poly1,\n".format(column_name=column_name)
            else:
                column_dict['attributes'].append('%s_rate_poly3' % column_name)
                another_name_list.append('%s_rate_poly3' % column_name)
                if column_type == 'string':
                    attributes_str += "round(count(if({column_name} is null or {column_name}='' or lower({column_name})='unknown',1,null))*100.0/count(1),2) as {column_name}_rate_poly3,\n".format(column_name=column_name)
                else:
                    attributes_str += "round(count(if({column_name}<=0,1,null))*100.0/count(1),2) as {column_name}_rate_poly3,\n".format(column_name=column_name)
        # 为了兼容t+2更新的表，where_day可能等于前天
        if ';' in table_partition:
            table_partition, where_day = table_partition.split(';')
        else:
            where_day = day
        where_sql = "%s where day='%s'" % (table_name, where_day) if 'day' in table_partition else '%s' % table_name
        column_sql = main_dimension_str + 'count(1) as row_num,\n' + uid_str + string_id_str + num_id_str + values_str + attributes_str
        another_name_list.append('row_num')
        # 删掉最后一个逗号和换行符
        column_sql = column_sql[:-2]
        dimension_sql, targets_sql = '', ''
        if len(column_dict['main_dimension']) > 0:
            main_dimension_str = ''
            for column_name in sorted((column_dict['main_dimension'])):
                main_dimension_str += '{column_name},\n'.format(column_name=column_name)
            base_sql = "select\n{column_sql}\nfrom {where_sql}\ngroup by\n{main_dimension_str}\n".format(column_sql=column_sql,where_sql=where_sql,main_dimension_str=main_dimension_str[:-2])
            for dimension in sorted((column_dict['main_dimension'])):
                # 维度拼接
                dimension_sql += "',{dimension}:',{dimension},".format(dimension=dimension)
        else:
            base_sql = "select\n{column_sql}\nfrom {where_sql}\n".format(column_sql=column_sql,where_sql=where_sql)
        for another_name in another_name_list:
            # 指标别名拼接
            targets_sql += "',{another_name}:',{another_name},".format(another_name=another_name)
        # 去掉维度和指标的第一个单引号和逗号，然后再把引号加回去
        if dimension_sql != '':
            dimension_sql = ",concat('" + dimension_sql[2:-1] + ") as dimension\n"
        else:
            dimension_sql = ",'' as dimension\n"
        if targets_sql != '':
            targets_sql = ",concat('" + targets_sql[2:-1] + ") as targets\n"
        else:
            targets_sql = ",'' as targets\n"
        format_sql = "select\n'{table_name}' as table_name\n" \
                     "{dimension_sql}" \
                     "{targets_sql}" \
                     "from\n(\n{base_sql}) t".format(table_name=table_name,dimension_sql=dimension_sql,targets_sql=targets_sql,base_sql=base_sql)
        uid_column = ','.join(column_dict['uid'])
        string_id_column = ','.join(column_dict['string_id'])
        num_id_column = ','.join(column_dict['num_id'])
        main_dimension_column = ','.join(sorted((column_dict['main_dimension'])))
        not_monitor_column = ','.join(column_dict['not_monitor'])
        values_column = ','.join(column_dict['values'])
        attributes_column = ','.join(column_dict['attributes'])
        data_list.append([app_name, table_name, table_owner,table_partition,table_size,table_update_time,query_times,day_size,downstream_user,uid_column
                          ,string_id_column,num_id_column,main_dimension_column,not_monitor_column,values_column,attributes_column,format_sql])
    pd_df = pd.DataFrame(data_list, columns=['app_name', 'table_name', 'table_owner','table_partition','table_size','table_update_time','query_times'
        ,'day_size','downstream_user','uid_column','string_id_column','num_id_column','main_dimension_column','not_monitor_column'
        ,'values_column','attributes_column','format_sql'])
    spark_df = sparkSession.createDataFrame(pd_df)
    spark_df.createOrReplaceTempView('monitor_column_table_sql_di_view')
    sparkSession.sql("insert overwrite table dwd.monitor_column_table_sql_di partition(day='%s') select * from monitor_column_table_sql_di_view" % day)


def create_task_and_run(day, task_flag):
    """
    create table if not exists dwd.monitor_column_task_info_di
    (
        task_id int comment '任务id'
        ,task_info string comment '任务内包含的table信息,包括业务线和表名,用逗号分隔'
        ,scan_size bigint comment '任务扫描的数据量,单位为字节'
        ,scan_num int comment '任务扫描表的数量'
        ,task_duration bigint comment '任务执行的时长,单位为秒'
        ,task_sql string comment '提交任务的sql'
    ) comment '指标监控的任务运行信息表'
    partitioned by
    (
        day string comment '天分区'
        ,task_flag string comment 'task标识,取值有early和else'
    )
    stored as orc
    tblproperties (
      'orc.compress'='snappy'
    );
    create table if not exists dwd.monitor_column_target_info_di
    (
        app_name string comment '业务名'
        ,table_name string comment '表名'
        ,table_owner string comment '表归属人'
        ,table_partition string comment '表分区信息,多个分区用逗号分隔'
        ,table_size bigint comment '表大小'
        ,query_times int comment '表昨日访问次数'
        ,day_size bigint comment '最近10日数据量均值,作为后续拼接sql的参考'
        ,downstream_user string comment '下游业务用户,用逗号分隔,只对数仓业务线有价值'
        ,dimension string comment '维度信息,多个维度用逗号分隔,维度名与值之间用冒号分隔'
        ,target_name string comment '指标名称'
        ,target_type string comment '指标类别,取值范围为:uid,string_id,num_id,values,attributes'
        ,target_value double comment '指标取值'
    ) comment '指标监控的天表'
    partitioned by
    (
        day string comment '天分区'
        ,task_id int comment '任务id分区,保证任务可重复提交'
    )
    stored as orc
    tblproperties (
      'orc.compress'='snappy'
    );
    """
    if task_flag == 'early':
        condition = ''
        task_id = 1
    else:
        condition = 'not'
        task_id = 1000
    # 刷历史数据时，先删除当日数据分区，重新构造task sql
    sql_str = """select * from dwd.monitor_column_table_sql_di 
                 where day='%s' and %s (table_update_time<'07:00:00' or table_partition REGEXP 'hour')
                 order by day_size,table_name""" % (day, condition)
    sparkSession = SparkSession.builder.appName('monitor_column_table_sql_di').enableHiveSupport().getOrCreate()
    hive_ret = sparkSession.sql(sql_str)
    ret = hive_ret.collect()
    input_list = []
    task_sql, scan_size, scan_num, task_duration, task_info_list = '', 0, 0, 0, []
    for r in ret:
        table_name = r.table_name
        day_size = r.day_size
        format_sql = r.format_sql
        # 每次至少union all5段sql,大于5段后，再看扫描的空间是否小于20G，且表数目小于20个
        if scan_num < 1 \
            or scan_num < 2 and scan_size < 1024 * 1024 * 1024 * 1024 \
            or scan_num < 4 and scan_size < 500 * 1024 * 1024 * 1024 \
            or scan_num < 9 and scan_size < 200 * 1024 * 1024 * 1024 \
            or scan_num < 14 and scan_size < 50 * 1024 * 1024 * 1024 \
            or scan_num < 19 and scan_size < 20 * 1024 * 1024 * 1024:
            scan_size += day_size
            scan_num += 1
            task_info_list.append(table_name)
            task_sql += '%s\nunion all\n' % format_sql
        else:
            scan_size += day_size
            scan_num += 1
            task_info_list.append(table_name)
            task_sql += '%s\nunion all\n' % format_sql
            task_sql_insert = """
            insert overwrite table dwd.monitor_column_target_info_di partition(day='%s', task_id=%i)
            select
            app_name
            ,tt.table_name
            ,table_owner
            ,table_partition
            ,table_size
            ,query_times
            ,day_size
            ,downstream_user
            ,dimension
            ,split(target_1,':')[0] as target_name
            ,case
            when uid_column regexp split(target_1,':')[0] then 'uid_column'
            when string_id_column regexp split(target_1,':')[0] then 'string_id_column'
            when values_column regexp split(target_1,':')[0] then 'values_column'
            when attributes_column regexp split(target_1,':')[0] then 'attributes_column'
            when num_id_column regexp split(target_1,':')[0] then 'num_id_column'
            else 'unknown'
            end as target_type
            ,split(target_1,':')[1] as target_value
            from
            (
                %s
            ) tt
            left join dwd.monitor_column_table_sql_di tt2 on tt.table_name=tt2.table_name
            LATERAL VIEW explode(split(targets, ',')) l2 as target_1
            where tt2.day='%s'
            """ % (day, task_id, task_sql[:-10], day)
            input_list.append([task_id, ','.join(task_info_list), scan_size, scan_num, task_sql_insert])
            # 再次初始化
            task_sql, scan_size, scan_num, task_duration, task_id, task_info_list = '', 0, 0, 0, task_id+1, []
    # 执行剩余的sql
    if len(task_sql) > 0:
        task_sql_insert = """
            insert overwrite table dwd.monitor_column_target_info_di partition(day='%s', task_id=%i)
            select
            app_name
            ,tt.table_name
            ,table_owner
            ,table_partition
            ,table_size
            ,query_times
            ,day_size
            ,downstream_user
            ,dimension
            ,split(target_1,':')[0] as target_name
            ,case
            when uid_column regexp split(target_1,':')[0] then 'uid_column'
            when string_id_column regexp split(target_1,':')[0] then 'string_id_column'
            when values_column regexp split(target_1,':')[0] then 'values_column'
            when attributes_column regexp split(target_1,':')[0] then 'attributes_column'
            when num_id_column regexp split(target_1,':')[0] then 'num_id_column'
            else 'unknown'
            end as target_type
            ,split(target_1,':')[1] as target_value
            from
            (
                %s
            ) tt
            left join dwd.monitor_column_table_sql_di tt2 on tt.table_name=tt2.table_name
            LATERAL VIEW explode(split(targets, ',')) l2 as target_1
            where tt2.day='%s'
            """ % (day, task_id, task_sql[:-10], day)
        input_list.append([task_id, ','.join(task_info_list), scan_size, scan_num, task_sql_insert])
    print 'input_list:'
    output_list = []
    if len(input_list) > 0:
        for info_list in input_list:
            task_id, task_info, scan_size, scan_num, task_sql = info_list
            current_seconds = int(time.time())
            sparkSession = SparkSession.builder.appName('monitor_column_target_info_di_%i' % task_id).enableHiveSupport().getOrCreate()
            sparkSession.sql(task_sql)
            task_duration = int(time.time()) - current_seconds
            print "task_id:%i\nscan_size:%s \n scan_num:%s\n task_duration:%s" % (task_id, scan_size, scan_num, task_duration)
            print 'task_sql_insert:\n', task_sql
            output_list.append([task_id, task_info, scan_size, scan_num, task_duration, task_sql])
        sparkSession = SparkSession.builder.appName('monitor_column_task_info_di').enableHiveSupport().getOrCreate()
        pd_df = pd.DataFrame(output_list,columns=['task_id', 'task_info', 'scan_size', 'scan_num', 'task_duration', 'task_sql'])
        spark_df = sparkSession.createDataFrame(pd_df)
        spark_df.createOrReplaceTempView('monitor_column_task_info_di_view')
        sparkSession.sql("insert overwrite table dwd.monitor_column_task_info_di partition(day='%s',task_flag='%s') select * from monitor_column_task_info_di_view" % (day, task_flag))




if __name__ == '__main__':
    day = sys.argv[1]
    task_flag = sys.argv[2]
    # 为每个表构造监控sql
    create_table_sql(day)
    # 根据表天分区数据量,小表多合并,大表少合并，提交任务串行运行，将结果插入到指标数据天表，并记录任务运行耗费时间到任务表
    create_task_and_run(day, task_flag)

