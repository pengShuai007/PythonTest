#!/usr/bin/env python
# coding=utf-8
from aliyunsdkcore import client
from aliyunsdkecs.request.v20140526 import DescribeInstancesRequest
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

sqlQuery = "SELECT * FROM asset_ecs_info WHERE ASSET_NO = %s"

basicSql = "INSERT INTO asset_basic_info (ASSET_NO,ACCOUNT,ASSET_TYPE,CREATE_TIME) VALUES (%s,%s,'0',NOW())"

updateBasicSql = "UPDATE asset_basic_info SET ACCOUNT = %s WHERE ASSET_NO = %s"

def save_data(info_list):
    for info in info_list:
        assetNo = info.get('InstanceId')
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
        createTime = info.get('CreationTime')
        expiredTime = info.get('ExpiredTime')

        queryParams = [assetNo]
        cur.execute(sqlQuery, queryParams)
        row = cur.fetchone()
        if row:
            if row[2] != zone or row[3] != region or row[4] != ecsType or row[5] != ecsName or row[6] != status \
                    or row[7] != innerIpAddress or row[8] != ipAddress or row[9] != mem or row[10] != cpu or row[
                11] != bandWidth \
                    or row[12] != createTime or row[13] != expiredTime:
                newJson = [{"assetNo": assetNo, "ZONE_ID": zone, "REGION": region, "ECS_TYPE": ecsType,
                            "ECS_NAME": ecsName, "STATUS": status, "INTRANET_IP": innerIpAddress,
                            "INTERNET_IP": ipAddress,
                            "MEM": mem, "CPU": cpu, "BANDWIDTH": bandWidth, "CREATE_TIME": createTime,
                            "EXPIRED_TIME": expiredTime}]
                oldJson = [{"assetNo": row[1], "ZONE_ID": row[2], "REGION": row[3], "ECS_TYPE": row[4],
                            "ECS_NAME": row[5], "STATUS": row[6], "INTRANET_IP": row[7], "INTERNET_IP": row[8],
                            "MEM": row[9], "CPU": row[10], "BANDWIDTH": row[11], "CREATE_TIME": row[12],
                            "EXPIRED_TIME": row[13]}]
                newJsonString = json.dumps(newJson, indent=2)
                oldJsonString = json.dumps(oldJson, indent=2)
                insertParams = [assetNo, oldJsonString, newJsonString]
                cur.execute(insertSql, insertParams)
                updateParams = [zone, region, ecsType, ecsName, status, innerIpAddress, ipAddress, mem, cpu, bandWidth,
                                createTime, expiredTime, assetNo]
                cur.execute(updateSql, updateParams)
            update_basic = [account,assetNo]
            cur.execute(updateBasicSql,update_basic)
        else:
            params = [assetNo, ecsName, region, zone, ecsType, cpu, mem, bandWidth, status, ipAddress, innerIpAddress,
                      createTime, expiredTime]
            cur.execute(sql, params)
            basicParam = [assetNo,account]
            cur.execute(basicSql, basicParam)
    conn.commit()

if __name__ == '__main__':
    conn = pymysql.connect(user=dbUser, passwd=dbPass,
                           host=dbHost, db=dbName, use_unicode=True, charset="utf8")
    cur = conn.cursor()
    regions = ["cn-qingdao", "cn-hangzhou", "cn-beijing", "cn-shanghai", "cn-shenzhen", "cn-zhangjiakou"]
    for region in regions:
        # 获得 ecs列表
        clt = client.AcsClient(accessKey, accessSecret, region)
        request = DescribeInstancesRequest.DescribeInstancesRequest()
        request.set_accept_format('json')
        request.set_PageSize(100)  # 每页条数
        request.set_PageNumber(1)  # 第几页
        # PageNumber, PageSize
        response = json.loads(clt.do_action_with_exception(request), encoding='utf-8')
        infoList = response.get('Instances').get('Instance')
        if infoList:
            save_data(infoList)
            total = response.get('TotalCount')
            num = int(round(total / 100.0))
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
                save_data(infoList)
    cur.close()
    conn.close()