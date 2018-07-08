# coding=utf8
import pymysql
import chardet
import pandas
import sys

# dbUser = sys.argv[1]
# dbPass = sys.argv[2]
# dbHost = sys.argv[3]
# dbName = sys.argv[4]
# file_path = sys.argv[5].decode('gbk').encode('utf8')
dbUser = 'root'
dbPass = 'dev123456'
dbHost = '118.190.21.49'
dbName = 'cop'
file_path = 'E:/消费明细/2017-01.csv'

try:
    data = open(unicode(file_path, 'utf-8'), 'r')
    target_code = chardet.detect(data.read()).get('encoding')
    csv_file = open(unicode(file_path, 'utf-8'))
    df = pandas.read_csv(csv_file, error_bad_lines=False, encoding=target_code)

    values = ''
    for row in df.values:
        month = row[0]
        asset = row[12]
        cost = row[22]
        cash = row[24]
        voucher = row[25]
        unSettle = row[26]
        row_value = r"('%s','%s','%s','%s','%s','%s')," % (month, asset, cost, cash, voucher, unSettle)
        values += row_value

    if values[-1] == ',':
        values = values[: -1]
    values.encode('utf-8')
    insertSql = "INSERT INTO asset_cost_record_detail (COST_MONTH,ASSET_NO,COST,CASH,VOUCHER,UNSETTLE_AMOUNT) " \
                "VALUES " + values
    insertSql.encode('utf-8')
    querySql = "SELECT RD_ID FROM asset_cost_record_detail LIMIT 1 "
    deleteSql = "DELETE FROM asset_cost_record_detail "

    conn = pymysql.connect(user=dbUser, passwd=dbPass,
                           host=dbHost, db=dbName, use_unicode=True, charset="utf8")

    cur = conn.cursor()
    cur.execute(querySql)
    row = cur.fetchone()
    if row:
        cur.execute(deleteSql)
    cur.execute(insertSql)

    sql = "INSERT INTO asset_cost_record (ASSET_NO,MONTH,COST_TYPE,COST,CASH,VOUCHER,UNSETTLE_AMOUNT,CREATE_TIME)" \
          "(SELECT ASSET_NO,MONTH,COST_TYPE,COST,CASH,VOUCHER,UNSETTLE_AMOUNT,CREATE_TIME FROM (SELECT ASSET_NO," \
          "REPLACE(REPLACE(COST_MONTH, '年', '-'),'月','') MONTH,1 COST_TYPE," \
          "CONVERT(SUM(COST), DECIMAL (9, 2)) COST," \
          "CONVERT(SUM(CASH), DECIMAL (9, 2)) CASH," \
          "CONVERT(SUM(VOUCHER), DECIMAL (9, 2)) VOUCHER," \
          "CONVERT(SUM(UNSETTLE_AMOUNT), DECIMAL (9, 2)) UNSETTLE_AMOUNT,NOW() CREATE_TIME," \
          "CONVERT((SUM(COST) + SUM(CASH) + SUM(VOUCHER) + SUM(UNSETTLE_AMOUNT)),DECIMAL (9, 2)) TOTAL " \
          "FROM asset_cost_record_detail GROUP BY ASSET_NO,COST_MONTH) a WHERE a.TOTAL > 0) "

    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

except Exception, e:
    print str(e)