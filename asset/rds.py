#!/usr/bin/env python
# coding=utf-8
from aliyunsdkcore import client
from aliyunsdkrds.request.v20140815 import DescribeDBInstancesRequest
import json
import pymysql
import sys
import math
import datetime

# dbUser = sys.argv[1]
# dbPass = sys.argv[2]
# dbHost = sys.argv[3]
# dbName = sys.argv[4]

UTC_FORMAT_C = "%Y-%m-%dT%H:%MZ"
UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

sql = "INSERT INTO asset_rds_info (ASSET_NO,INSTANCE_TYPE,ZONE_ID,REGION,RDS_NAME,ENGINE,ENGINE_VSERSION," \
      "PAY_TYPE,RDS_NET_TYPE,RDS_CLASS,RDS_STATUS,CREATE_TIME,EXPIRE_TIME) VALUES " \
      "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

sqlQuery = "SELECT ASSET_NO,INSTANCE_TYPE,ZONE_ID,REGION,RDS_NAME,ENGINE,ENGINE_VSERSION,PAY_TYPE,RDS_NET_TYPE," \
           "RDS_CLASS,RDS_STATUS,CREATE_TIME,EXPIRE_TIME FROM asset_rds_info WHERE ASSET_NO = %s"

insertSql = "INSERT INTO asset_change_record (ASSET_NO,CHANGE_TPYE,OLD,NEW,CREATE_TIME) " \
            "VALUES (%s,'0',%s,%s,NOW())"

updateSql = "UPDATE asset_rds_info SET INSTANCE_TYPE = %s,ZONE_ID = %s,REGION=%s,RDS_NAME=%s,ENGINE=%s," \
            "ENGINE_VSERSION=%s,PAY_TYPE=%s,RDS_NET_TYPE=%s,RDS_CLASS=%s,RDS_STATUS =%s,CREATE_TIME=%s," \
            "EXPIRE_TIME=%s WHERE ASSET_NO = %s "

basicSql = "INSERT INTO asset_basic_info (ASSET_NO,ACCOUNT,ASSET_TYPE,CREATE_TIME) VALUES (%s,%s,'1',NOW())"

updateBasicSql = "UPDATE asset_basic_info SET ACCOUNT = %s WHERE ASSET_NO = %s"

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)

def save_or_update_data(rds_info, account):
    for info in rds_info:
        rds_no = info.get('DBInstanceId')
        sync_instances.append(rds_no)
        rds_name = info.get('DBInstanceDescription')
        region = info.get('RegionId')
        zone = info.get('ZoneId')
        engine = info.get('Engine')
        engineVersion = info.get('EngineVersion')
        payType = info.get('PayType')
        rdsNetType = info.get('DBInstanceNetType')
        rdsClass = info.get('DBInstanceClass')
        rdsStatus = info.get('DBInstanceStatus')
        createTimeUTC = info.get('CreateTime')
        expiredTimeUTC = info.get('ExpireTime')
        if len(createTimeUTC) > 0:
            creteTime = str(datetime.datetime.strptime(createTimeUTC, UTC_FORMAT_C)).decode('utf-8')
        if len(expiredTimeUTC) > 0:
            expireTime = str(datetime.datetime.strptime(expiredTimeUTC, UTC_FORMAT)).decode('utf-8')
        # 临时实例不存入数据库
        db_instance_type = info.get('DBInstanceType')
        if 'Temp' != db_instance_type:
            queryParams = [rds_no]
            cur.execute(sqlQuery, queryParams)
            row = cur.fetchone()
            if row:
                if row[1] != db_instance_type or row[2] != zone or row[3] != region or row[4] != rds_name or \
                        row[5] != engine or row[6] != engineVersion or row[7] != payType or row[8] != rdsNetType \
                        or row[9] != rdsClass or row[10] != rdsStatus or row[11] != creteTime or row[12] != expireTime:
                    newJson = [{"assetNo": rds_no,"INSTANCE_TYPE": db_instance_type, "ZONE_ID": zone, "REGION": region, "RDS_NAME": rds_name,
                                "ENGINE": engine, "ENGINE_VSERSION": engineVersion, "PAY_TYPE": payType,
                                "RDS_NET_TYPE": rdsNetType,
                                "RDS_CLASS": rdsClass, "RDS_STATUS": rdsStatus, "CREATE_TIME": creteTime,
                                "EXPIRE_TIME": expireTime}]
                    oldJson = [{"assetNo": row[1],"INSTANCE_TYPE":row[1], "ZONE_ID": row[2], "REGION": row[3], "RDS_NAME": row[4],
                                "ENGINE": row[5], "ENGINE_VSERSION": row[6], "PAY_TYPE": row[7],
                                "RDS_NET_TYPE": row[8],
                                "RDS_CLASS": row[9], "RDS_STATUS": row[10], "CREATE_TIME": row[11],
                                "EXPIRE_TIME": row[12]}]
                    newJsonString = json.dumps(newJson, indent=2, cls=DateEncoder)
                    oldJsonString = json.dumps(oldJson, indent=2, cls=DateEncoder)
                    insertParams = [rds_no, oldJsonString, newJsonString]
                    cur.execute(insertSql, insertParams)
                    updateParams = [db_instance_type,zone, region, rds_name, engine, engineVersion, payType, rdsNetType, rdsClass, rdsStatus,
                                    creteTime, expireTime, rds_no]
                    cur.execute(updateSql, updateParams)
                update_basic = [account, rds_no]
                cur.execute(updateBasicSql, update_basic)
            else:
                params = [rds_no, db_instance_type, zone, region, rds_name, engine, engineVersion, payType, rdsNetType, rdsClass, rdsStatus,
                          creteTime, expireTime]
                cur.execute(sql, params)
                basicParams = [rds_no, account]
                cur.execute(basicSql, basicParams)

def deal_data(access_key,access_secret,account):
    regions = ["cn-qingdao", "cn-hangzhou", "cn-beijing", "cn-shanghai", "cn-shenzhen", "cn-zhangjiakou"]
    for region in regions:
        # 获得cn-qingdao rds列表
        clt = client.AcsClient(access_key, access_secret, region)
        request = DescribeDBInstancesRequest.DescribeDBInstancesRequest()
        request.set_accept_format('json')
        request.set_PageSize(100)  # 每页条数
        request.set_PageNumber(1)  # 第几页
        response = json.loads(clt.do_action_with_exception(request), encoding='utf-8')
        rds_info = response.get('Items').get('DBInstance')
        if rds_info:
            total = response.get('TotalRecordCount')
            print '同步rds----账号：'+ account + '；区域：' + region + "；总数：" + str(total)
            save_or_update_data(rds_info, account)
            num = int(math.ceil(total / 100.0))
            index = 1
            while index < num:
                index = index + 1
                request = DescribeDBInstancesRequest.DescribeDBInstancesRequest()
                request.set_accept_format('json')
                request.set_PageSize(100)  # 每页条数
                request.set_PageNumber(index)  # 第几页
                # PageNumber, PageSize
                response = json.loads(clt.do_action_with_exception(request), encoding='utf-8')
                rds_info = response.get('Items').get('DBInstance')
                save_or_update_data(rds_info, account)

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
    sql_local = 'SELECT asset_no FROM asset_basic_info where ASSET_TYPE = 1 AND ACCOUNT in ' + account_str
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
        access_key = str(access.get('ACCESS_KEY'))
        access_secret = str(access.get('ACCESS_SECRET'))
        account = str(access.get('ACCOUNT'))
        account_list.append(account)
        deal_data(access_key.strip(), access_secret.strip(), account)
    update_local()  # 如果本次同步不包含已经存在的资产，则更新资产状态为已释放
    conn.commit()
    cur.close()
    conn.close()