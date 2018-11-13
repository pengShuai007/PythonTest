# -*- coding: utf-8 -*-
import oss2
import os

endpoint = "oss-cn-qingdao.aliyuncs.com"
accesskey_id = "xxxxx"
accesskey_secret = "xxxx"
bucket_name = "ky-group-prd"

# 本地文件保存路径前缀
download_local_save_prefix = "F:/download_oss/"

'''
列举prefix全部文件
'''


def prefix_all_list(bucket, prefix):
    print("开始列举" + prefix + "全部文件")
    oss_file_size = 0
    for obj in oss2.ObjectIterator(bucket, prefix='%s/' % prefix):
        oss_file_size = oss_file_size + 1
        download_to_local(bucket, obj.key, obj.key)

    print(prefix + " file size " + str(oss_file_size))


'''
列举全部的根目录文件夹、文件
'''


def root_directory_list(bucket):
    # 设置Delimiter参数为正斜线（/）。
    for obj in oss2.ObjectIterator(bucket, delimiter='/'):
        # 通过is_prefix方法判断obj是否为文件夹。
        if obj.is_prefix():  # 文件夹
            print('directory: ' + obj.key)
            prefix_all_list(bucket, str(obj.key).strip("/"))   # 去除/
        else:  # 文件
            print('file: ' + obj.key)
            # 下载根目录的单个文件
            download_to_local(bucket, str(obj.key), str(obj.key))


'''
下载文件到本地
'''


def download_to_local(bucket, object_name, local_file):
    url = download_local_save_prefix + local_file
    # 文件名称
    file_name = url[url.rindex("/") + 1:]

    file_path_prefix = url.replace(file_name, "")
    if False == os.path.exists(file_path_prefix):
        os.makedirs(file_path_prefix)
        print("directory don't not makedirs " + file_path_prefix)

    # 下载OSS文件到本地文件。如果指定的本地文件存在会覆盖，不存在则新建。
    bucket.get_object_to_file(object_name, download_local_save_prefix + local_file)


if __name__ == '__main__':
    print("start \n")
    # 阿里云主账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM账号进行API访问或日常运维，请登录 https://ram.console.aliyun.com 创建RAM账号。
    auth = oss2.Auth(accesskey_id, accesskey_secret)
    bucket = oss2.Bucket(auth, endpoint, bucket_name)
    root_directory_list(bucket)
    print("end \n")
