#!/usr/bin/env python
# coding=utf-8
from aliyunsdkcore import client
from aliyunsdkrds.request.v20140815 import DescribeDBInstancesRequest
import json
import pymysql
import sys

dbUser = sys.argv[1]
dbPass = sys.argv[2]
dbHost = sys.argv[3]
dbName = sys.argv[4]
accessKey = sys.argv[5]
accessSecret = sys.argv[6]
account = sys.argv[7].decode('gbk').encode('utf8')

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

def save_or_update_data(rds_info):
    for info in rds_info:
        rds_no = info.get('DBInstanceId');
        rds_name = info.get('DBInstanceDescription')
        region = info.get('RegionId')
        zone = info.get('ZoneId')
        engine = info.get('Engine')
        engineVersion = info.get('EngineVersion')
        payType = info.get('PayType')
        rdsNetType = info.get('DBInstanceNetType')
        rdsClass = info.get('DBInstanceClass')
        rdsStatus = info.get('DBInstanceStatus')
        creteTime = info.get('CreateTime')
        expireTime = info.get('ExpireTime')
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
                    newJsonString = json.dumps(newJson, indent=2)
                    oldJsonString = json.dumps(oldJson, indent=2)
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
                basicParams = [rds_no,account]
                cur.execute(basicSql, basicParams)
    conn.commit()

if __name__ == '__main__':
    conn = pymysql.connect(user=dbUser, passwd=dbPass,
                           host=dbHost, db=dbName, use_unicode=True, charset="utf8")
    cur = conn.cursor()

    regions = ["cn-qingdao", "cn-hangzhou", "cn-beijing", "cn-shanghai", "cn-shenzhen", "cn-zhangjiakou"]
    for region in regions:
        # 获得cn-qingdao ecs列表
        clt = client.AcsClient(accessKey, accessSecret, region)
        request = DescribeDBInstancesRequest.DescribeDBInstancesRequest()
        request.set_accept_format('json')
        request.set_PageSize(100)  # 每页条数
        request.set_PageNumber(1)  # 第几页
        response = json.loads(clt.do_action_with_exception(request), encoding='utf-8')
        rds_info = response.get('Items').get('DBInstance')
        if rds_info:
            save_or_update_data(rds_info)
            total = response.get('TotalRecordCount')
            num = int(round(total / 100.0))
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
                save_or_update_data(rds_info)
    cur.close()
    conn.close()