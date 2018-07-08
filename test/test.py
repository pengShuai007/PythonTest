# coding=utf8

file_path = 'E:\消费明细\2017-01.csv'

print('123')
try:
    data = open(unicode(file_path, 'utf-8'), 'r')
    print ('正确打开')
except Exception, e:
    print str(e)