#!/usr/bin/env python
# coding=utf-8
from aliyunsdkcore import client
from aliyunsdkecs.request.v20140526 import DescribeInstancesRequest
import json
import pymysql
import sys
import math
import datetime

# dbUser = sys.argv[1]
# dbPass = sys.argv[2]
# dbHost = sys.argv[3]
# dbName = sys.argv[4]


UTC_FORMAT = "%Y-%m-%dT%H:%MZ"

sql = "insert into asset_ecs_info " + \
      " (ASSET_NO,ECS_NAME,REGION,ZONE_ID,ECS_TYPE,CPU,MEM, " + \
      " BANDWIDTH,STATUS,INTERNET_IP,INTRANET_IP," + \
      " CREATE_TIME,EXPIRED_TIME ) " + \
      " values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "

updateSql = "update asset_ecs_info set ZONE_ID=%s,REGION=%s,ECS_TYPE=%s,ECS_NAME=%s," \
            "STATUS=%s,INTRANET_IP=%s,INTERNET_IP=%s,MEM=%s,CPU=%s,BANDWIDTH=%s," \
            "CREATE_TIME=%s,EXPIRED_TIME=%s where ASSET_NO = %s"

insertSql = "INSERT INTO asset_change_record (ASSET_NO,CHANGE_TPYE,OLD,NEW,CREATE_TIME) " \
            "VALUES (%s,'0',%s,%s,NOW())"

sqlQuery = "SELECT asset_no,zone_id,region,ecs_type,ecs_name,STATUS,intranet_ip,internet_ip,mem,cpu,bandwidth," \
           "create_time,expired_time FROM asset_ecs_info WHERE ASSET_NO = %s"

basicSql = "INSERT INTO asset_basic_info (ASSET_NO,ACCOUNT,ASSET_TYPE,CREATE_TIME) VALUES (%s,%s,'0',NOW())"


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


def save_data(info_list, account):
    for info in info_list:
        assetNo = info.get('InstanceId')
        sync_instances.append(assetNo)
        ecsName = info.get('InstanceName')
        region = info.get('RegionId')
        zone = info.get('ZoneId')
        ecsType = info.get('InstanceType')
        cpu = str(info.get('Cpu'))
        mem = str(info.get('Memory'))
        bandWidth = str(info.get('InternetMaxBandwidthOut'));
        status = info.get('Status')
        if info.get('PublicIpAddress').get('IpAddress'):
            ipAddress = info.get('PublicIpAddress').get('IpAddress')
            if ipAddress:
                ipAddress = ipAddress[0]
        else:
            ipAddress = ''
        innerIpAddress = info.get('InnerIpAddress').get('IpAddress')
        if innerIpAddress:
            innerIpAddress = innerIpAddress[0]
        else:
            innerIpAddress = ''
        createTimeUTC = info.get('CreationTime')
        expiredTimeUTC = info.get('ExpiredTime')
        createTime = datetime.datetime.strptime(createTimeUTC, UTC_FORMAT)
        expiredTime = datetime.datetime.strptime(expiredTimeUTC, UTC_FORMAT)

        queryParams = [assetNo]
        cur.execute(sqlQuery, queryParams)
        row = cur.fetchone()
        if row:
            if row[1] != zone or row[2] != region or row[3] != ecsType or row[4] != ecsName or row[5] != status \
                    or row[6] != innerIpAddress or row[7] != ipAddress or row[8] != mem or row[9] != cpu \
                    or row[10] != bandWidth or row[11] != createTime or row[12] != expiredTime:
                newJson = [{"assetNo": assetNo, "ZONE_ID": zone, "REGION": region, "ECS_TYPE": ecsType,
                            "ECS_NAME": ecsName, "STATUS": status, "INTRANET_IP": innerIpAddress,
                            "INTERNET_IP": ipAddress,
                            "MEM": mem, "CPU": cpu, "BANDWIDTH": bandWidth, "CREATE_TIME": createTime,
                            "EXPIRED_TIME": expiredTime}]
                oldJson = [{"assetNo": row[0], "ZONE_ID": row[1], "REGION": row[2], "ECS_TYPE": row[3],
                            "ECS_NAME": row[4], "STATUS": row[5], "INTRANET_IP": row[6], "INTERNET_IP": row[7],
                            "MEM": row[8], "CPU": row[9], "BANDWIDTH": row[10], "CREATE_TIME": row[11],
                            "EXPIRED_TIME": row[12]}]
                newJsonString = json.dumps(newJson, indent=2, cls=DateEncoder, ensure_ascii=False)
                oldJsonString = json.dumps(oldJson, indent=2, cls=DateEncoder, ensure_ascii=False)
                insertParams = [assetNo, oldJsonString, newJsonString]
                cur.execute(insertSql, insertParams)
                updateParams = [zone, region, ecsType, ecsName, status, innerIpAddress, ipAddress, mem, cpu, bandWidth,
                                createTime, expiredTime, assetNo]
                cur.execute(updateSql, updateParams)
        else:
            params = [assetNo, ecsName, region, zone, ecsType, cpu, mem, bandWidth, status, ipAddress, innerIpAddress,
                      createTime, expiredTime]
            cur.execute(sql, params)
            basicParam = [assetNo,account]
            cur.execute(basicSql, basicParam)


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

def deal_data(access_key,access_secret,account):
    regions = ["cn-qingdao", "cn-hangzhou", "cn-beijing", "cn-shanghai", "cn-shenzhen", "cn-zhangjiakou"]
    for region in regions:
        # 获得 ecs列表
        clt = client.AcsClient(access_key, access_secret, region)
        request = DescribeInstancesRequest.DescribeInstancesRequest()
        request.set_accept_format('json')
        request.set_PageSize(100)  # 每页条数
        request.set_PageNumber(1)  # 第几页
        # PageNumber, PageSize
        response = json.loads(clt.do_action_with_exception(request), encoding='utf-8')
        infoList = response.get('Instances').get('Instance')
        if infoList:
            total = response.get('TotalCount')
            print '同步ecs----账号：'+ account + '；区域：' + region + "；总数：" + str(total)
            save_data(infoList, account)
            num = int(math.ceil(total / 100.0))
            index = 1
            while index < num:
                index = index + 1
                request = DescribeInstancesRequest.DescribeInstancesRequest()
                request.set_accept_format('json')
                request.set_PageSize(100)  # 每页条数
                request.set_PageNumber(index)  # 第几页
                # PageNumber, PageSize
                response = json.loads(clt.do_action_with_exception(request), encoding='utf-8')
                infoList = response.get('Instances').get('Instance')
                save_data(infoList, account)


def update_local():
    account_str = ''
    for account in account_list:
        account_str += "'" + account + "'"
    if len(account_str) > 0:
        if account_str[-1] == ',':
            account_str = account_str[: -1]
        account_str = '(' + account_str + ')'
    sql_local = 'SELECT asset_no FROM asset_basic_info where ASSET_TYPE = 0 AND ACCOUNT in ' + account_str
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