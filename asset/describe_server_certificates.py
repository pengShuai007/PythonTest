# coding=utf8

from aliyunsdkcore import client
from aliyunsdkslb.request.v20140515 import DescribeServerCertificatesRequest
import json
import datetime


'''
查询已上传的服务器证书信息
'''

UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

def save_data(cert_list):
    for cert in cert_list:
        server_cert_id = cert.get('ServerCertificateId')    # 服务器证书ID
        server_cert_name = cert.get('ServerCertificateName')  # 服务器证书名称
        fingerprint = cert.get('Fingerprint')  # 服务器证书指纹
        cert_time_utc = cert.get('CreateTime')   # 证书上传时间
        if len(cert_time_utc) > 0:
            create_time = str(datetime.datetime.strptime(cert_time_utc, UTC_FORMAT)).decode('utf-8')
        cert_time_stmp = cert.get('CreateTimeStamp')  # 证书上传时间戳
        is_ali_cert = cert.get('IsAliCloudCertificate')  # 是否阿里证书，0代表不是
        ali_cert_name = cert.get('AliCloudCertificateName')  # 阿里证书名称
        ali_cert_id = cert.get('AliCloudCertificateId')   #阿里证书ID
        expiretime_utc = cert.get('ExpireTime')  # 过期时间
        if len(expiretime_utc) > 0 :
            expiretime = str(datetime.datetime.strptime(expiretime_utc, UTC_FORMAT)).decode('utf-8')
        expiretimestamp = cert.get('ExpireTimeStamp')  # 过期时间戳
        CommonName = cert.get('CommonName')  # 域名
        ResourceGroupId = cert.get('ResourceGroupId')  # 实例的企业资源组ID
        RegionId = cert.get('RegionId')  # 负载均衡实例的地域
        sql_query = ''



def deal_data():
    access_key = ''
    access_secret = ''
    clt = client.AcsClient(access_key, access_secret, 'cn-qingdao')
    request = DescribeServerCertificatesRequest.DescribeServerCertificatesRequest()
    request.set_accept_format('json')
    response = json.loads(clt.do_action_with_exception(request), encoding='utf-8')
    # response = json.dumps(clt.do_action_with_exception(request))
    # ojt = json.loads(response).encode('utf-8')
    cert_list = response.get('ServerCertificates')
    if cert_list:
        save_data(cert_list)


if __name__ == '__main__':
    print deal_data()