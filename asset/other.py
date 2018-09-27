#!/usr/bin/env python
# coding=utf-8
from aliyunsdkcore import client
from aliyunsdkr_kvstore.request.v20150101 import DescribeInstancesRequest
from aliyunsdkdds.request.v20151201 import DescribeDBInstancesRequest
from aliyunsdkdts.request.v20160801 import DescribeSynchronizationJobsRequest
import json
import pymysql
import sys
import math
import datetime

# dbUser = sys.argv[1]
# dbPass = sys.argv[2]
# dbHost = sys.argv[3]
# dbName = sys.argv[4]


UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
UTC_FORMAT_C = "%Y-%m-%dT%H:%MZ"

sql = "INSERT INTO asset_other_info (ASSET_NO,REGION,RDS_NAME,ENGINE_INFO," \
      "CHARGE_TYPE,RDS_STATUS,CREATE_TIME,EXPIRE_TIME) VALUES " \
      "(%s,%s,%s,%s,%s,%s,%s,%s)"

sqlQuery = "SELECT asset_no,region,rds_name,engine_info,charge_type,rds_status,create_time,expire_time FROM " \
           "asset_other_info WHERE ASSET_NO = %s "

insertSql = "INSERT INTO asset_change_record (ASSET_NO,CHANGE_TPYE,OLD,NEW,CREATE_TIME) " \
            "VALUES (%s,'0',%s,%s,NOW())"

updateSql = "UPDATE asset_other_info SET REGION=%s,RDS_NAME=%s,ENGINE_INFO=%s," \
            "CHARGE_TYPE=%s,RDS_STATUS =%s,CREATE_TIME=%s,EXPIRE_TIME=%s " \
            "WHERE ASSET_NO = %s"

basicSql = "INSERT INTO asset_basic_info (ASSET_NO,ACCOUNT,ASSET_TYPE,CREATE_TIME) VALUES (%s,%s,'3',NOW())"


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)

def save_data(info_list):
    for info in info_list:
        assetNo = info.get('InstanceId')
        sync_instances.append(assetNo)
        rds_name = info.get('InstanceName')
        region = info.get('RegionId')
        rdsClass = info.get('InstanceClass')
        chargeType = info.get('ChargeType')
        rdsStatus = info.get('InstanceStatus')
        create_time_utc = info.get('CreateTime')
        expire_time_utc = info.get('EndTime')
        if len(create_time_utc) > 0:
            creteTime = str(datetime.datetime.strptime(create_time_utc, UTC_FORMAT)).decode('utf-8')
        if len(expire_time_utc) > 0:
            expireTime = str(datetime.datetime.strptime(expire_time_utc, UTC_FORMAT)).decode('utf-8')

        engine_info = {'class': rdsClass}
        engineInfo = json.dumps(engine_info)

        queryParams = [assetNo]
        cur.execute(sqlQuery, queryParams)
        row = cur.fetchone()
        if row:
            if row[1] != region or row[2] != rds_name or row[4] != chargeType or row[3] != engineInfo \
                    or row[5] != rdsStatus or row[6] != creteTime or row[7] != expireTime:
                newJson = [{"assetNo": assetNo, "REGION": region, "RDS_NAME": rds_name, "CHARGE_TYPE": chargeType,
                            "ENGINE_INFO": engineInfo, "RDS_STATUS": rdsStatus, "CREATE_TIME": creteTime,
                            "EXPIRE_TIME": expireTime}]
                oldJson = [{"assetNo": row[0], "REGION": row[1], "RDS_NAME": row[2], "CHARGE_TYPE": row[4],
                            "ENGINE_INFO": row[3], "RDS_STATUS": row[5], "CREATE_TIME": row[6],
                            "EXPIRE_TIME": row[7]}]
                newJsonString = json.dumps(newJson, indent=2, cls=DateEncoder, ensure_ascii=False)
                oldJsonString = json.dumps(oldJson, indent=2, cls=DateEncoder, ensure_ascii=False)
                insertParams = [assetNo, oldJsonString, newJsonString]
                cur.execute(insertSql, insertParams)
                updateParams = [region, rds_name, engineInfo, chargeType, rdsStatus,
                                creteTime, expireTime, assetNo]
                cur.execute(updateSql, updateParams)
        else:
            params = [assetNo, region, rds_name, engineInfo, chargeType, rdsStatus,
                      creteTime, expireTime]
            cur.execute(sql, params)
            basicParams = [assetNo, account]
            cur.execute(basicSql, basicParams)


def save_or_update_data(other_info):
    for info in other_info:
        engine = info.get('Engine')
        engineVersion = info.get('EngineVersion')
        rdsClass = info.get('DBInstanceClass')
        rds_no = info.get('DBInstanceId')
        sync_instances.append(rds_no)
        rds_name = info.get('DBInstanceDescription')
        region = info.get('RegionId')
        chargeType = info.get('ChargeType')
        rdsStatus = info.get('DBInstanceStatus')
        creteTime = info.get('CreationTime')
        expire_time_utc = info.get('ExpireTime')
        # if len(create_time_utc) > 0:
        #     creteTime = str(datetime.datetime.strptime(create_time_utc, UTC_FORMAT)).decode('utf-8')
        if len(expire_time_utc) > 0:
            expireTime = str(datetime.datetime.strptime(expire_time_utc, UTC_FORMAT_C)).decode('utf-8')

        engine_info = {'type': engine, 'version': engineVersion, 'class': rdsClass}
        engineInfo = json.dumps(engine_info)

        queryParams = [rds_no]
        cur.execute(sqlQuery, queryParams)
        row = cur.fetchone()
        if row:
            if row[1] != region or row[2] != rds_name or row[4] != chargeType or row[3] != engineInfo \
                    or row[5] != rdsStatus or row[6] != creteTime or row[7] != expireTime:
                newJson = [{"assetNo": rds_no, "REGION": region, "RDS_NAME": rds_name, "CHARGE_TYPE": chargeType,
                            "ENGINE_INFO": engineInfo, "RDS_STATUS": rdsStatus, "CREATE_TIME": creteTime,
                            "EXPIRE_TIME": expireTime}]
                oldJson = [{"assetNo": row[0], "REGION": row[1], "RDS_NAME": row[2], "CHARGE_TYPE": row[4],
                            "ENGINE_INFO": row[3], "RDS_STATUS": row[5], "CREATE_TIME": row[6], "EXPIRE_TIME": row[7]}]
                newJsonString = json.dumps(newJson, indent=2, cls=DateEncoder, ensure_ascii=False)
                oldJsonString = json.dumps(oldJson, indent=2, cls=DateEncoder, ensure_ascii=False)
                insertParams = [rds_no, oldJsonString, newJsonString]
                cur.execute(insertSql, insertParams)
                updateParams = [region, rds_name, engineInfo, chargeType, rdsStatus,
                                creteTime, expireTime, rds_no]
                cur.execute(updateSql, updateParams)
        else:
            params = [rds_no, region, rds_name, engineInfo, chargeType, rdsStatus,
                      creteTime, expireTime]
            cur.execute(sql, params)
            basicParams = [rds_no, account]
            cur.execute(basicSql, basicParams)


def save_dts(dts_list):
    for info in dts_list:
        assetNo = info.get('SynchronizationJobId')
        sync_instances.append(assetNo)
        rds_name = info.get('SynchronizationJobName')
        chargeType = info.get('PayType')
        rdsClass = info.get('SynchronizationJobClass')
        rdsStatus = info.get('Status')
        expire_time_utc = info.get('ExpireTime')
        if len(expire_time_utc) > 0:
            expireTime = str(datetime.datetime.strptime(expire_time_utc, UTC_FORMAT)).decode('utf-8')

        engine_info = {'class': rdsClass}
        engineInfo = json.dumps(engine_info)

        queryParams = [assetNo]
        cur.execute(sqlQuery, queryParams)
        row = cur.fetchone()
        if row:
            if row[2] != rds_name or row[4] != chargeType or row[3] != engineInfo or row[5] != rdsStatus \
                    or row[7] != expireTime:
                newJson = [{"assetNo": assetNo, "RDS_NAME": rds_name, "CHARGE_TYPE": chargeType,
                            "ENGINE_INFO": engineInfo, "RDS_STATUS": rdsStatus, "EXPIRE_TIME": expireTime}]
                oldJson = [{"assetNo": row[0], "RDS_NAME": row[2], "CHARGE_TYPE": row[4],
                            "ENGINE_INFO": row[3], "RDS_STATUS": row[5], "EXPIRE_TIME": row[7]}]
                newJsonString = json.dumps(newJson, indent=2, cls=DateEncoder, ensure_ascii=False)
                oldJsonString = json.dumps(oldJson, indent=2, cls=DateEncoder, ensure_ascii=False)
                insertParams = [assetNo, oldJsonString, newJsonString]
                cur.execute(insertSql, insertParams)
                updateParams = ['', rds_name, engineInfo, chargeType, rdsStatus, '', expireTime, assetNo]
                cur.execute(updateSql, updateParams)
        else:
            params = [assetNo, '', rds_name, engineInfo, chargeType, rdsStatus, '', expireTime]
            cur.execute(sql, params)
            basicParams = [assetNo, account]
            cur.execute(basicSql, basicParams)


def deal_data_redis(clt, region):
    request_redis = DescribeInstancesRequest.DescribeInstancesRequest()
    request_redis.set_accept_format('json')
    request_redis.set_PageSize(50)  # 每页条数
    request_redis.set_PageNumber(1)  # 第几页
    # PageNumber, PageSize
    response = json.loads(clt.do_action_with_exception(request_redis), encoding='utf-8')
    infoList = response['Instances']['KVStoreInstance']
    if infoList:
        total = response.get('TotalCount')
        print '同步redis----账号：' + account + "；区域：" + region + "；总数：" + str(total)
        save_data(infoList)
        num = int(math.ceil(total / 50.0))
        index = 1
        while index < num:
            index = index + 1
            request_redis = DescribeInstancesRequest.DescribeInstancesRequest()
            request_redis.set_accept_format('json')
            request_redis.set_PageSize(50)  # 每页条数
            request_redis.set_PageNumber(index)  # 第几页
            # PageNumber, PageSize
            response = json.loads(clt.do_action_with_exception(request_redis), encoding='utf-8')
            infoList = response.get('Instances').get('Instance')
            save_data(infoList)


def deal_data_mongodb(clt, region):
    request_mongodb = DescribeDBInstancesRequest.DescribeDBInstancesRequest()
    request_mongodb.set_accept_format('json')
    request_mongodb.set_PageSize(100)
    response = json.loads(clt.do_action_with_exception(request_mongodb), encoding='utf-8')
    other_info = response.get('DBInstances').get('DBInstance')
    if other_info:
        total = response.get('TotalCount')
        print '同步mongodb----账号：' + account + '；区域：' + region + "；总数：" + str(total)
        save_or_update_data(other_info)
        num = int(math.ceil(total / 100.0))
        index = 1
        while index < num:
            index = index + 1
            request_mongodb = DescribeDBInstancesRequest.DescribeDBInstancesRequest()
            request_mongodb.set_accept_format('json')
            request_mongodb.set_PageSize(100)
            request_mongodb.set_PageNumber(index)
            response = json.loads(clt.do_action_with_exception(request_mongodb), encoding='utf-8')
            other_info = response.get('DBInstances').get('DBInstance')
            save_or_update_data(other_info)


def deal_data_dts():
    clt_dts = client.AcsClient(access_key, access_secret)
    request_dts = DescribeSynchronizationJobsRequest.DescribeSynchronizationJobsRequest()
    request_dts.set_accept_format('json')
    request_dts.set_PageSize(100)  # 本页同步实例数
    request_dts.set_PageNum(1)  # 第几页
    response = json.loads(clt_dts.do_action_with_exception(request_dts), encoding='utf-8')
    dts_list = response['SynchronizationInstances']
    if dts_list:
        total = response.get('TotalRecordCount')
        print '同步dts----账号：' + account + "；总数：" + str(total)
        save_dts(dts_list)
        num = int(math.ceil(total / 100.0))
        index = 1
        while index < num:
            index = index + 1
            request_dts = DescribeSynchronizationJobsRequest.DescribeSynchronizationJobsRequest()
            request_dts.set_accept_format('json')
            request_dts.PageSize(100)  # 本页同步实例数
            request_dts.set_PageNum(index)  # 第几页
            response = json.loads(clt_dts.do_action_with_exception(request_dts), encoding='utf-8')
            dts_list = response['SynchronizationInstances']
            save_dts(dts_list)


def deal_data():
    regions = ["cn-qingdao", "cn-hangzhou", "cn-beijing", "cn-shanghai", "cn-shenzhen", "cn-zhangjiakou"]
    for region in regions:
        # 获得cn-qingdao MongoDB列表
        clt = client.AcsClient(access_key, access_secret, region)
        deal_data_mongodb(clt, region)
        deal_data_redis(clt, region)


def get_access():
    get_sql = "SELECT ALI_UUID,ACCOUNT,KEYID,KEYSECRET FROM asset_account_info WHERE state = 1 "
    cur.execute(get_sql)
    data = cur.fetchall()
    access_result = []
    if data:
        for row in data:
            access_dict = {}
            access_dict['ALI_UUID'] = row[0]
            access_dict["ACCOUNT"] = row[1]
            access_dict["ACCESS_KEY"] = row[2]
            access_dict["ACCESS_SECRET"] = row[3]
            access_result.append(access_dict)
        return access_result


def update_local():
    account_str = ''
    for account in account_list:
        account_str += "'" + account + "'"
    if len(account_str) > 0:
        if account_str[-1] == ',':
            account_str = account_str[: -1]
        account_str = '(' + account_str + ')'
    sql_local = 'SELECT asset_no FROM asset_basic_info where ASSET_TYPE = 3 AND ACCOUNT in ' + account_str
    cur.execute(sql_local)
    data = cur.fetchall()
    update_instance = ''
    if data:
        for row in data:
            asset_no = row[0]  # 数据库中已有的资产编号
            if asset_no not in sync_instances:
                update_instance += "'" + asset_no + "',"
        if len(update_instance) > 0:
            if update_instance[-1] == ',':
                update_instance = update_instance[: -1]
            update_instance = '(' + update_instance + ')'
            # 更新资产状态为已释放
            update_sql = "UPDATE asset_basic_info SET ASSET_STATE = '3' WHERE asset_no IN " + update_instance
            cur.execute(update_sql)


if __name__ == '__main__':
    conn = pymysql.connect(user=dbUser, passwd=dbPass,
                           host=dbHost, db=dbName, use_unicode=True, charset="utf8")
    cur = conn.cursor()
    access_list = get_access()
    sync_instances = []
    account_list = []
    for access in access_list:
        ali_uuid = str(access.get('ALI_UUID'))
        access_key = str(access.get('ACCESS_KEY')).strip()
        access_secret = str(access.get('ACCESS_SECRET')).strip()
        account = str(access.get('ACCOUNT'))
        account_list.append(account)
        # 同步dts信息
        deal_data_dts()
        # 同步MongoDB、Redis信息
        deal_data()
    update_local()  # 如果本次同步不包含已经存在的资产，则更新资产状态为已释放
    conn.commit()
    cur.close()
    conn.close()