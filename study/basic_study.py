# coding=utf-8
import time

def yang():
    print ("/a")  # 响铃
    print (r"/a")  # 原样输出
    # 字符串格式化
    print "My name is %s and weight is %d kg!" % ('Yang', 70)
    a = '''杨鹏
        所见即所得
        '''
    print a

def deal_month(month):
    if month < 10:
        return '0' + str(month)
    else:
        return str(month)

def get_last_month():
    times = time.localtime(time.time())
    tm_year = times.tm_year
    tm_mon = times.tm_mon
    tm_day = times.tm_mday
    tm_hour = times.tm_hour
    if tm_day >= 4 and tm_hour >= 12:
        last_month = str(tm_year) + '-' + deal_month(tm_mon)
    else:
        last_month = str(tm_year) + '-' + deal_month(tm_mon - 1)
    return last_month

if __name__ == "__main__":
    months = []
    for month in months:
        print month

