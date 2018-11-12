# coding=utf8

import urllib,urllib2
import time
import uuid
import sys
import hashlib
import hmac
import base64
import json
import pymysql
import math
import datetime

# dbUser = sys.argv[1]
# dbPass = sys.argv[2]
# dbHost = sys.argv[3]
# dbName = sys.argv[4]

'''
获取资产消费信息，即什么时候花钱
'''

# 获取请求时间戳
def get_Timestamp():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# 获取唯一随机数，防止网络攻击
def get_SignatureNonce():
    return str(uuid.uuid4())

# 拼接参数字典
def get_all_parameters(accessKeyId,billingCycle,pageNumber,pageSize):
    parameters = {
        "Format": "JSON",
        "Version": "2017-12-14",
        "AccessKeyId": accessKeyId,
        "SignatureMethod": "HMAC-SHA1",
        "Timestamp": get_Timestamp(),
        "SignatureVersion": "1.0",
        "SignatureNonce": get_SignatureNonce(),
        "Action": "QueryMonthlyInstanceConsumption",
        "BillingCycle": billingCycle,
        "PageNum": pageNumber,
        "PageSize": pageSize
    }
    return parameters


def __pop_standard_urlencode(query):
    ret = query.replace('+', '%20')
    ret = ret.replace('*', '%2A')
    ret = ret.replace('%7E', '~')
    return ret


def __compose_string_to_sign(method, queries):
    sorted_parameters = sorted(queries.items(), key=lambda queries: queries[0])
    sorted_query_string = __pop_standard_urlencode(urllib.urlencode(sorted_parameters))
    canonicalized_query_string = __pop_standard_urlencode(urllib.pathname2url(sorted_query_string))
    string_to_sign = method + "&%2F&" + canonicalized_query_string
    return string_to_sign


# 获取签名字符串
def get_Signature(parameters,accessKeySecret):
    if 'Signature' in parameters:
        del parameters['Signature']
    string_to_sign = __compose_string_to_sign('GET', parameters)
    if isinstance(accessKeySecret, unicode):
        accessKeySecret = str(accessKeySecret)
    accessKeySecret += '&'
    # 计算签名HMAC值
    h = hmac.new(accessKeySecret, string_to_sign, hashlib.sha1)
    # 按照Base64 编码规则把上面的HMAC值编码成字符串
    signature = base64.encodestring(h.digest()).strip()
    return signature

def get_response(accessKeyId,accessSecret,billingCycle,pageNumber,pageSize):
    url = 'http://business.aliyuncs.com'
    parameters = get_all_parameters(accessKeyId,billingCycle,pageNumber,pageSize)
    signature = get_Signature(parameters,accessSecret)
    parameters["Signature"] = signature
    print parameters
    parameters = urllib.urlencode(parameters)
    req = urllib2.Request(url='%s%s%s' % (url, '?', parameters))
    res = urllib2.urlopen(req)
    res = res.read()
    return res


def save_detail_data(ali_uuid, billing_cycle, cost_info):
    insert_value = ''
    for info in cost_info:
        instance_id = info.get('InstanceID')
        product_code = info.get('ProductCode')
        pretax_gross_amount = info.get('PretaxGrossAmount')  # 原始金额
        discount_amount = info.get('DiscountAmount')  # 整体优惠金额
        pretax_amount = info.get('PretaxAmount')  # 优惠后金额
        if pretax_gross_amount != 0.0:
            row_value = r"('%s','%s','%s','%s','%s','%s','%s')," % (ali_uuid, instance_id, product_code,
                                                billing_cycle, pretax_gross_amount, discount_amount, pretax_amount)
            insert_value += row_value
    if insert_value[-1] == ',':
        insert_value = insert_value[: -1]
    if len(insert_value) > 1:  # 如果有需要插入的数据
        insert_detail_sql = 'INSERT INTO asset_consume_info (ALI_UUID,INSTANCE_ID,PRODUCT_CODE,BILLING_CYCLE,' \
                            'PRETAX_GROSS_AMOUNT,DISCOUNT_AMOUNT,PRETAX_AMOUNT) VALUES ' + insert_value
        cur.execute(insert_detail_sql)


# 月份处理，小于十月前面添加0
def deal_month(month):
    if month < 10:
        return '0' + str(month)
    else:
        return str(month)

def get_last_month():
    times = time.localtime(time.time())
    tm_year = times.tm_year
    tm_mon = times.tm_mon
    last_month = str(tm_year) + '-' + deal_month(tm_mon - 1)
    return last_month

def add_months(dt,months):
    month = dt.month - 1 + months
    year = dt.year + month / 12
    month = month % 12 + 1
    return dt.replace(year=year, month=month)

def get_between_month(begin_date,end_date):
    date_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m")
        date_list.append(date_str)
        begin_date = add_months(begin_date, 1)
    return date_list

# 获取需要同步的月份数组，默认从18年1月开始
def get_months():
    begin_month = '2018-01'
    last_month = get_last_month()
    get_sql = 'select billing_cycle from asset_consume_info where ali_uuid = %s order by billing_cycle desc limit 1 '
    query_params = [ali_uuid]
    cur.execute(get_sql, query_params)
    row = cur.fetchone()
    if row:
        begin_month = row[0]  # 数据库查出来的月份，不能包括这个月
        temp_month = datetime.datetime.strptime(begin_month, "%Y-%m")
        begin_month = add_months(temp_month, 1).strftime("%Y-%m")
    months = get_between_month(begin_month, last_month)
    return months

def get_access():
    get_sql = "SELECT ALI_UUID,KEYID,KEYSECRET FROM asset_account_info WHERE state = 1 "
    cur.execute(get_sql)
    data = cur.fetchall()
    access_result = []
    if data:
        for row in data:
            access_dict = {}
            access_dict['ALI_UUID'] = row[0]
            access_dict["ACCESS_KEY"] = row[1]
            access_dict["ACCESS_SECRET"] = row[2]
            access_result.append(access_dict)
        return access_result

def deal_data(ali_uuid,access_key,access_secret,month):
    res = get_response(access_key, access_secret, month, 1, 100)
    res = json.loads(res, encoding='utf-8')
    cost_info = res.get('Data').get('Items').get('Item')
    if cost_info:
        save_detail_data(ali_uuid, month, cost_info)
        total_count = res.get('Data').get('TotalCount')
        num = int(math.ceil(total_count / 100.0))
        index = 1
        while index < num:
            index += 1
            res = get_response(access_key, access_secret, month, index, 100)
            res = json.loads(res, encoding='utf-8')
            cost_info = res.get('Data').get('Items').get('Item')
            save_detail_data(ali_uuid, month, cost_info)

if __name__ == '__main__':
    conn = pymysql.connect(user=dbUser, passwd=dbPass,
                           host=dbHost, db=dbName, use_unicode=True, charset="utf8")
    cur = conn.cursor()

    access_list = get_access()
    for access in access_list:
        ali_uuid = str(access.get('ALI_UUID'))
        access_key = str(access.get('ACCESS_KEY'))
        access_secret = str(access.get('ACCESS_SECRET'))
        month_list = get_months()
        for month in month_list:
            deal_data(ali_uuid, access_key.strip(), access_secret.strip(), month)
    # 每个账号每个月的明细数据插入以后再提交
    conn.commit()
    cur.close()
    conn.close()