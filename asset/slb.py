#!/usr/bin/env python
# coding=utf-8
from aliyunsdkcore import client
from aliyunsdkslb.request.v20140515 import DescribeLoadBalancersRequest
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

sqlQuery = "SELECT * FROM asset_slb_info WHERE ASSET_NO = %s"

sql = "INSERT INTO asset_slb_info (ASSET_NO,REGION,SLB_NAME,ADDR,ADDR_TYPE,SLB_STATUS,NETWORK_TYPE,BANDWIDTH," \
      "CREATE_TIME,MASTER_ZONE,SLAVE_ZONE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

insertSql = "INSERT INTO asset_change_record (ASSET_NO,CHANGE_TPYE,OLD,NEW,CREATE_TIME) " \
            "VALUES (%s,'0',%s,%s,NOW())"

updateSql = "UPDATE asset_slb_info SET REGION = %s,SLB_NAME = %s,ADDR = %s,ADDR_TYPE = %s,SLB_STATUS = %s," \
            "NETWORK_TYPE = %s,BANDWIDTH = %s,CREATE_TIME = %s,MASTER_ZONE = %s,SLAVE_ZONE = %s WHERE ASSET_NO = %s"

basicSql = "INSERT INTO asset_basic_info (ASSET_NO,ACCOUNT,ASSET_TYPE,CREATE_TIME) VALUES (%s,%s,'2',NOW())"

updateBasicSql = "UPDATE asset_basic_info SET ACCOUNT = %s WHERE ASSET_NO = %s"

def save_or_update_data(slb_info):
    for info in slb_info:
        slb_no = info.get('LoadBalancerId')
        slb_name = info.get('LoadBalancerName')
        slb_status = info.get('LoadBalancerStatus')
        slb_address = info.get('Address')
        address_type = info.get('AddressType')
        region = info.get('RegionId')
        network_type = info.get('NetworkType')
        bandwidth = info.get('Bandwidth')
        create_time = info.get('CreateTime')
        masterZone = info.get('MasterZoneId')
        slaveZone = info.get('SlaveZoneId')

        paramQuery = [slb_no]
        cur.execute(sqlQuery, paramQuery)
        row = cur.fetchone()
        if row:
            if row[2] != region or row[3] != slb_name or row[4] != slb_address or row[5] != address_type or row[
                6] != slb_status or row[7] != network_type or row[8] != bandwidth or row[9] != create_time or row[
                10] != masterZone or row[11] != slaveZone:
                newJson = [{"assetNo": slb_no, "REGION": region, "SLB_NAME": slb_name, "ADDR": slb_address,
                            "ADDR_TYPE": address_type, "SLB_STATUS": slb_status, "NETWORK_TYPE": network_type,
                            "BANDWIDTH": bandwidth, "CREATE_TIME": create_time, "MASTER_ZONE": masterZone,
                            "SLAVE_ZONE": slaveZone}]
                oldJson = [{"assetNo": row[1], "REGION": row[2], "SLB_NAME": row[3], "ADDR": row[4],
                            "ADDR_TYPE": row[5], "SLB_STATUS": row[6], "NETWORK_TYPE": row[7],
                            "BANDWIDTH": row[8], "CREATE_TIME": row[9], "MASTER_ZONE": row[10],
                            "SLAVE_ZONE": row[11]}]
                newJsonString = json.dumps(newJson, indent=2)
                oldJsonString = json.dumps(oldJson, indent=2)
                insertParams = [slb_no, oldJsonString, newJsonString]
                cur.execute(insertSql, insertParams)
                updateParams = [region, slb_name, slb_address, address_type, slb_status, network_type, bandwidth,
                                create_time, masterZone, slaveZone, slb_no]
                cur.execute(updateSql, updateParams)
            update_basic = [account, slb_no]
            cur.execute(updateBasicSql, update_basic)
        else:
            params = [slb_no, region, slb_name, slb_address, address_type, slb_status, network_type, bandwidth,
                      create_time,
                      masterZone, slaveZone]
            cur.execute(sql, params)
            basicParams = [slb_no,account]
            cur.execute(basicSql, basicParams)
    conn.commit()

if __name__ == '__main__':
    conn = pymysql.connect(user=dbUser, passwd=dbPass,
                           host=dbHost, db=dbName, use_unicode=True, charset="utf8")
    cur = conn.cursor()

    regions = ["cn-qingdao", "cn-hangzhou", "cn-beijing", "cn-shanghai", "cn-shenzhen", "cn-zhangjiakou"]
    for region in regions:
        clt = client.AcsClient(accessKey, accessSecret, region)
        request = DescribeLoadBalancersRequest.DescribeLoadBalancersRequest()
        request.set_accept_format('json')
        request.set_PageSize(100)
        request.set_PageNumber(1)
        response = json.loads(clt.do_action_with_exception(request), encoding='utf-8')
        slb_info = response.get('LoadBalancers').get('LoadBalancer')
        if slb_info:
            save_or_update_data(slb_info)
            total = response.get('TotalCount')
            num = int(round(total / 100.0))
            index = 1
            while index < num:
                index = index + 1
                request = DescribeLoadBalancersRequest.DescribeLoadBalancersRequest()
                request.set_accept_format('json')
                request.set_PageSize(100)  # 每页条数
                request.set_PageNumber(index)  # 第几页
                # PageNumber, PageSize
                response = json.loads(clt.do_action_with_exception(request), encoding='utf-8')
                slb_info = response.get('LoadBalancers').get('LoadBalancer')
                save_or_update_data(slb_info)
    cur.close()
    conn.close()