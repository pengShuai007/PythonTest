#!/usr/bin/env python
# coding=utf-8
from aliyunsdkcore import client
from aliyunsdkslb.request.v20140515 import DescribeLoadBalancersRequest
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


sqlQuery = "SELECT asset_no,region,slb_name,addr,addr_type,slb_status,network_type,bandwidth,create_time," \
           "master_zone,slave_zone,resource_group_id FROM asset_slb_info WHERE ASSET_NO = %s"

sql = "INSERT INTO asset_slb_info (ASSET_NO,REGION,SLB_NAME,ADDR,ADDR_TYPE,SLB_STATUS,NETWORK_TYPE,BANDWIDTH," \
      "CREATE_TIME,MASTER_ZONE,SLAVE_ZONE,RESOURCE_GROUP_ID) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

insertSql = "INSERT INTO asset_change_record (ASSET_NO,CHANGE_TPYE,OLD,NEW,CREATE_TIME) " \
            "VALUES (%s,'0',%s,%s,NOW())"

updateSql = "UPDATE asset_slb_info SET REGION = %s,SLB_NAME = %s,ADDR = %s,ADDR_TYPE = %s,SLB_STATUS = %s," \
            "NETWORK_TYPE = %s,BANDWIDTH = %s,CREATE_TIME = %s,MASTER_ZONE = %s,SLAVE_ZONE = %s," \
            "RESOURCE_GROUP_ID = %s WHERE ASSET_NO = %s"

basicSql = "INSERT INTO asset_basic_info (ASSET_NO,ACCOUNT,ASSET_TYPE,CREATE_TIME) VALUES (%s,%s,'2',NOW())"


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


def save_or_update_data(slb_info, account):
    for info in slb_info:
        slb_no = info.get('LoadBalancerId')
        slb_name = info.get('LoadBalancerName')
        slb_status = info.get('LoadBalancerStatus')
        slb_address = info.get('Address')
        address_type = info.get('AddressType')
        region = info.get('RegionId')
        network_type = info.get('NetworkType')
        bandwidth = info.get('Bandwidth')
        create_time_utc = info.get('CreateTime')
        if len(create_time_utc) > 0:
            create_time = str(datetime.datetime.strptime(create_time_utc, UTC_FORMAT)).decode('utf-8')
        masterZone = info.get('MasterZoneId')
        slaveZone = info.get('SlaveZoneId')
        resource_group_id = info.get('ResourceGroupId')
        paramQuery = [slb_no]
        cur.execute(sqlQuery, paramQuery)
        row = cur.fetchone()
        if row:
            if row[1] != region or row[2] != slb_name or row[3] != slb_address or row[4] != address_type \
                    or row[5] != slb_status or row[6] != network_type or row[7] != bandwidth or row[8] != create_time \
                    or row[9] != masterZone or row[10] != slaveZone or row[11] != resource_group_id:
                newJson = [{"assetNo": slb_no, "REGION": region, "SLB_NAME": slb_name, "ADDR": slb_address,
                            "ADDR_TYPE": address_type, "SLB_STATUS": slb_status, "NETWORK_TYPE": network_type,
                            "BANDWIDTH": bandwidth, "CREATE_TIME": create_time, "MASTER_ZONE": masterZone,
                            "SLAVE_ZONE": slaveZone, "RESOURCE_GROUP_ID": resource_group_id}]
                oldJson = [{"assetNo": row[0], "REGION": row[1], "SLB_NAME": row[2], "ADDR": row[3],
                            "ADDR_TYPE": row[4], "SLB_STATUS": row[5], "NETWORK_TYPE": row[6],
                            "BANDWIDTH": row[7], "CREATE_TIME": row[8], "MASTER_ZONE": row[9],
                            "SLAVE_ZONE": row[10], "RESOURCE_GROUP_ID": row[11]}]
                newJsonString = json.dumps(newJson, indent=2, cls=DateEncoder)
                oldJsonString = json.dumps(oldJson, indent=2, cls=DateEncoder)
                insertParams = [slb_no, oldJsonString, newJsonString]
                cur.execute(insertSql, insertParams)
                updateParams = [region, slb_name, slb_address, address_type, slb_status, network_type, bandwidth,
                                create_time, masterZone, slaveZone, resource_group_id, slb_no]
                cur.execute(updateSql, updateParams)
        else:
            params = [slb_no, region, slb_name, slb_address, address_type, slb_status, network_type, bandwidth,
                      create_time,
                      masterZone, slaveZone, resource_group_id]
            cur.execute(sql, params)
            basicParams = [slb_no, account]
            cur.execute(basicSql, basicParams)


def deal_data(access_key,access_secret,account):
    regions = ["cn-qingdao", "cn-hangzhou", "cn-beijing", "cn-shanghai", "cn-shenzhen", "cn-zhangjiakou"]
    for region in regions:
        clt = client.AcsClient(access_key, access_secret, region)
        request = DescribeLoadBalancersRequest.DescribeLoadBalancersRequest()
        request.set_accept_format('json')
        request.set_PageSize(100)
        request.set_PageNumber(1)
        response = json.loads(clt.do_action_with_exception(request), encoding='utf-8')
        slb_info = response.get('LoadBalancers').get('LoadBalancer')
        if slb_info:
            total = response.get('TotalCount')
            print '同步slb----账号：'+ account + '；区域：' + region + "；总数：" + str(total)
            save_or_update_data(slb_info, account)
            num = int(math.ceil(total / 100.0))
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
                save_or_update_data(slb_info, account)

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
    sql_local = 'SELECT asset_no FROM asset_basic_info where ASSET_TYPE = 2 AND ACCOUNT in ' + account_str
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