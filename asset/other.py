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

dbUser = sys.argv[1]
dbPass = sys.argv[2]
dbHost = sys.argv[3]
dbName = sys.argv[4]
accessKey = sys.argv[5]
accessSecret = sys.argv[6]
account = sys.argv[7].decode('gbk').encode('utf8')

sql = "INSERT INTO asset_other_info (ASSET_NO,REGION,RDS_NAME,ENGINE_INFO," \
      "CHARGE_TYPE,RDS_STATUS,CREATE_TIME,EXPIRE_TIME) VALUES " \
      "(%s,%s,%s,%s,%s,%s,%s,%s)"

sqlQuery = "SELECT * FROM asset_other_info WHERE ASSET_NO = %s"

insertSql = "INSERT INTO asset_change_record (ASSET_NO,CHANGE_TPYE,OLD,NEW,CREATE_TIME) " \
            "VALUES (%s,'0',%s,%s,NOW())"

updateSql = "UPDATE asset_other_info SET REGION=%s,RDS_NAME=%s,ENGINE_INFO=%s," \
            "CHARGE_TYPE=%s,RDS_STATUS =%s,CREATE_TIME=%s,EXPIRE_TIME=%s " \
            "WHERE ASSET_NO = %s"

basicSql = "INSERT INTO asset_basic_info (ASSET_NO,ACCOUNT,ASSET_TYPE,CREATE_TIME) VALUES (%s,%s,'3',NOW())"

updateBasicSql = "UPDATE asset_basic_info SET ACCOUNT = %s WHERE ASSET_NO = %s"


def save_data(info_list):
    for info in info_list:
        assetNo = info.get('InstanceId')
        rds_name = info.get('InstanceName')
        region = info.get('RegionId')
        rdsClass = info.get('InstanceClass')
        chargeType = info.get('ChargeType')
        rdsStatus = info.get('InstanceStatus')
        creteTime = info.get('CreateTime')
        expireTime = info.get('EndTime')

        engine_info = {'class': rdsClass}
        engineInfo = json.dumps(engine_info)
        # engineInfo = str(engine_info1)

        queryParams = [assetNo]
        cur.execute(sqlQuery, queryParams)
        row = cur.fetchone()
        if row and (row[2] != region or row[3] != rds_name or row[
            4] != chargeType or row[5] != engineInfo or row[6] != rdsStatus or \
                                row[7] != creteTime or row[8] != expireTime):
            newJson = [{"assetNo": assetNo, "REGION": region, "RDS_NAME": rds_name,
                        "CHARGE_TYPE": chargeType,
                        "ENGINE_INFO": engineInfo, "RDS_STATUS": rdsStatus, "CREATE_TIME": creteTime,
                        "EXPIRE_TIME": expireTime}]
            oldJson = [{"assetNo": row[1], "REGION": row[2], "RDS_NAME": row[3],
                        "CHARGE_TYPE": row[4],
                        "ENGINE_INFO": row[5], "RDS_STATUS": row[6], "CREATE_TIME": row[7],
                        "EXPIRE_TIME": row[8]}]
            newJsonString = json.dumps(newJson, indent=2)
            oldJsonString = json.dumps(oldJson, indent=2)
            insertParams = [assetNo, oldJsonString, newJsonString]
            cur.execute(insertSql, insertParams)
            updateParams = [region, rds_name, engineInfo, chargeType, rdsStatus,
                            creteTime, expireTime, assetNo]
            cur.execute(updateSql, updateParams)
            update_basic = [account,assetNo]
            cur.execute(updateBasicSql,update_basic)
        else:
            params = [assetNo, region, rds_name, engineInfo, chargeType, rdsStatus,
                      creteTime, expireTime]
            cur.execute(sql, params)
            basicParams = [assetNo, account]
            cur.execute(basicSql, basicParams)
        conn.commit()


def save_or_update_data(other_info):
    for info in other_info:
        engine = info.get('Engine')
        engineVersion = info.get('EngineVersion')
        rdsClass = info.get('DBInstanceClass')
        rds_no = info.get('DBInstanceId')
        rds_name = info.get('DBInstanceDescription')
        region = info.get('RegionId')
        chargeType = info.get('ChargeType')
        rdsStatus = info.get('DBInstanceStatus')
        creteTime = info.get('CreationTime')
        expireTime = info.get('ExpireTime')

        engine_info = {'type': engine, 'version': engineVersion, 'class': rdsClass}
        engineInfo = json.dumps(engine_info)
        # engineInfo = str(engine_info1)

        queryParams = [rds_no]
        cur.execute(sqlQuery, queryParams)
        row = cur.fetchone()
        if row and (row[2] != region or row[3] != rds_name or \
                                row[4] != chargeType or row[5] != engineInfo or row[6] != rdsStatus or \
                                row[7] != creteTime or row[8] != expireTime):
            newJson = [{"assetNo": rds_no, "REGION": region, "RDS_NAME": rds_name,
                        # "ENGINE": engine, "ENGINE_VERSION": engineVersion,
                        "CHARGE_TYPE": chargeType,
                        "ENGINE_INFO": engineInfo, "RDS_STATUS": rdsStatus, "CREATE_TIME": creteTime,
                        "EXPIRE_TIME": expireTime}]
            oldJson = [{"assetNo": row[1], "REGION": row[2], "RDS_NAME": row[3],
                        # "ENGINE": row[4], "ENGINE_VERSION": row[5],
                        "CHARGE_TYPE": row[4],
                        "ENGINE_INFO": row[5], "RDS_STATUS": row[6], "CREATE_TIME": row[7],
                        "EXPIRE_TIME": row[8]}]
            newJsonString = json.dumps(newJson, indent=2)
            oldJsonString = json.dumps(oldJson, indent=2)
            insertParams = [rds_no, oldJsonString, newJsonString]
            cur.execute(insertSql, insertParams)
            updateParams = [region, rds_name, engineInfo, chargeType, rdsStatus,
                            creteTime, expireTime, rds_no]
            cur.execute(updateSql, updateParams)
            update_basic = [account,rds_no]
            cur.execute(updateBasicSql,update_basic)
        else:
            params = [rds_no, region, rds_name, engineInfo, chargeType, rdsStatus,
                      creteTime, expireTime]
            cur.execute(sql, params)
            basicParams = [rds_no, account]
            cur.execute(basicSql, basicParams)
        conn.commit()


def save_dts(dts_list):
    for info in dts_list:
        assetNo = info.get('SynchronizationJobId')
        rds_name = info.get('SynchronizationJobName')
        chargeType = info.get('PayType')
        rdsClass = info.get('SynchronizationJobClass')
        rdsStatus = info.get('Status')
        expireTime = info.get('ExpireTime')
        region = ''
        creteTime = ''

        engine_info = {'class': rdsClass}
        engineInfo = json.dumps(engine_info)

        queryParams = [assetNo]
        cur.execute(sqlQuery, queryParams)
        row = cur.fetchone()
        if row and (row[2] != region or row[3] != rds_name or row[
            4] != chargeType or row[5] != engineInfo or row[6] != rdsStatus or \
                                row[7] != creteTime or row[8] != expireTime):
            newJson = [{"assetNo": assetNo, "REGION": region, "RDS_NAME": rds_name,
                        "CHARGE_TYPE": chargeType,
                        "ENGINE_INFO": engineInfo, "RDS_STATUS": rdsStatus, "CREATE_TIME": creteTime,
                        "EXPIRE_TIME": expireTime}]
            oldJson = [{"assetNo": row[1], "REGION": row[2], "RDS_NAME": row[3],
                        "CHARGE_TYPE": row[4],
                        "ENGINE_INFO": row[5], "RDS_STATUS": row[6], "CREATE_TIME": row[7],
                        "EXPIRE_TIME": row[8]}]
            newJsonString = json.dumps(newJson, indent=2)
            oldJsonString = json.dumps(oldJson, indent=2)
            insertParams = [assetNo, oldJsonString, newJsonString]
            cur.execute(insertSql, insertParams)
            updateParams = [region, rds_name, engineInfo, chargeType, rdsStatus,
                            creteTime, expireTime, assetNo]
            cur.execute(updateSql, updateParams)
            update_basic = [account,assetNo]
            cur.execute(updateBasicSql,update_basic)
        else:
            params = [assetNo, region, rds_name, engineInfo, chargeType, rdsStatus,
                      creteTime, expireTime]
            cur.execute(sql, params)
            basicParams = [assetNo, account]
            cur.execute(basicSql, basicParams)
        conn.commit()


def redis(request_mongodb):
        request_mongodb.set_accept_format('json')
        request_mongodb.set_PageSize(50)  # 每页条数
        request_mongodb.set_PageNumber(1)  # 第几页
        # PageNumber, PageSize
        response = json.loads(clt.do_action_with_exception(request_mongodb), encoding='utf-8')
        infoList = response['Instances']['KVStoreInstance']
        if infoList:
            save_data(infoList)
            total = response.get('TotalCount')
            num = int(math.ceil(total / 50.0))
            index = 1
            while index < num:
                index = index + 1
                request_mongodb = DescribeInstancesRequest.DescribeInstancesRequest()
                request_mongodb.set_accept_format('json')
                request_mongodb.set_PageSize(50)  # 每页条数
                request_mongodb.set_PageNumber(index)  # 第几页
                # PageNumber, PageSize
                response = json.loads(clt.do_action_with_exception(request_mongodb), encoding='utf-8')
                infoList = response.get('Instances').get('Instance')
                save_data(infoList)


def mongoDB(request_redis):
        request_redis.set_accept_format('json')
        response = json.loads(clt.do_action_with_exception(request_redis), encoding='utf-8')
        other_info = response.get('DBInstances').get('DBInstance')
        if other_info:
            save_or_update_data(other_info)


def dts(request_dts):
        request_dts.set_accept_format('json')
        request_dts.set_PageSize(30)  # 本页同步实例数
        request_dts.set_PageNum(1)  # 第几页
        response = json.loads(clt_dts.do_action_with_exception(request_dts), encoding='utf-8')
        dts_list = response['SynchronizationInstances']
        if dts_list:
            save_dts(dts_list)
            total = response.get('TotalRecordCount')
            num = int(math.ceil(total / 30.0))
            index = 1
            while index < num:
                index = index + 1
                request_dts = DescribeSynchronizationJobsRequest.DescribeSynchronizationJobsRequest()
                request_dts.set_accept_format('json')
                request_dts.PageSize(30)  # 本页同步实例数
                request_dts.set_PageNum(1)  # 第几页
                response = json.loads(clt.do_action_with_exception(request_dts), encoding='utf-8')
                dts_list = response['SynchronizationInstances']
                save_dts(dts_list)


if __name__ == '__main__':
    conn = pymysql.connect(user=dbUser, passwd=dbPass,
                           host=dbHost, db=dbName, use_unicode=True, charset="utf8")
    cur = conn.cursor()

    regions = ["cn-qingdao", "cn-hangzhou", "cn-beijing", "cn-shanghai", "cn-shenzhen", "cn-zhangjiakou"]
    for region in regions:
        # 获得cn-qingdao MongoDB列表
        clt = client.AcsClient(accessKey, accessSecret, region)
        clt_dts = client.AcsClient(accessKey, accessSecret)
        request_mongodb = DescribeInstancesRequest.DescribeInstancesRequest()
        request_redis = DescribeDBInstancesRequest.DescribeDBInstancesRequest()
        request_dts = DescribeSynchronizationJobsRequest.DescribeSynchronizationJobsRequest()
        mongoDB(request_redis)
        redis(request_mongodb)
        dts(request_dts)

    cur.close()
    conn.close()