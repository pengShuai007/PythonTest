# coding=utf8
import time
import uuid
import urllib
import sys
import hashlib
import hmac
import base64

FORMAT_ISO_8601 = "%Y-%m-%dT%H:%M:%SZ"

# 获取请求时间戳
def get_Timestamp():
    return time.strftime(FORMAT_ISO_8601, time.gmtime())

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
        "Action": "QueryInstanceGaapCost",
        "BillingCycle": billingCycle,
        # "ProductType": "",
        "SubscriptionType": "Subscription",
        # "ProductCode": "",
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

if __name__ == '__main__':
    accessKeyId = 'LTAI0NnjkErT4Ypp'
    accessKeySecret = 'lycox8X2TH1l5puQH34v8tLc4RitxV'
    billingCycle = '2018-08'
    pageNumber = 1
    pageSize = 10
    parameters = get_all_parameters(accessKeyId,billingCycle,pageNumber,pageSize)
    print get_Signature(parameters,accessKeySecret)