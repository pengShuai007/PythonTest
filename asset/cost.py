# coding=utf8
import pymysql
import pandas
import sys


# 读取csv文件，将结果拼接成insert语句values
def file_to_value():
    try:
        csv_file = open(unicode(file_path, 'utf-8'), 'r')
        df = pandas.read_csv(csv_file, error_bad_lines=False)
        insert_value = ''
        for row in df.values:
            all_uuid = row[1]  # 阿里云帐号ID
            month = row[0]  # 费用发生月份
            asset = row[11]  # 资产编号
            asset_name = row[12]  # 资源昵称
            product = row[5]  # 阿里云产品
            cost_time = row[2]  # 消费时间
            process_id = row[4]  # 账单编号
            cost = row[21]  # 应付金额
            cash = row[23]  # 现金
            voucher = row[24]  # 代金卷
            unpaid = row[25]  # 未付金额
            row_value = r"('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')," % \
                        (all_uuid, month, asset, asset_name, product, cost_time, process_id,
                         cost, cash, voucher, unpaid)
            insert_value += row_value
        if insert_value[-1] == ',':
            insert_value = insert_value[: -1]
    except Exception, e:
        print str(e)
    return insert_value


# 保存csv文件内容到detail表
def save_detail():
    insert_sql = "INSERT INTO asset_cost_record_detail (ALI_UUID,COST_MONTH,ASSET_NO,ASSET_NAME,PRODUCT,COST_TIME," \
                 "PROCESSID,COST,CASH,VOUCHER,UNSETTLE_AMOUNT) VALUES " + values
    cur.execute(insert_sql)


# 整合detail表数据到record表
def save_record():
    save_sql = "INSERT INTO asset_cost_record (ALI_UUID,ASSET_NO,RES_ID,RES_NAME,PRODUCT,COST,CASH,VOUCHER," \
               "UNSETTLE_AMOUNT,MONTH,CREATE_TIME) (SELECT ALI_UUID,ASSET_NO,RES_ID,RES_NAME,PRODUCT,COST,CASH," \
               "VOUCHER,UNSETTLE_AMOUNT,MONTH,CREATE_TIME FROM (SELECT ALI_UUID,ASSET_NO,ASSET_NO RES_ID," \
               "ASSET_NAME RES_NAME,PRODUCT,CONVERT(SUM(COST),DECIMAL(9,2)) COST," \
               "CONVERT(SUM(CASH),DECIMAL(9,2)) CASH,CONVERT(SUM(VOUCHER),DECIMAL(9,2)) VOUCHER," \
               "CONVERT(SUM(UNSETTLE_AMOUNT),DECIMAL(9,2)) UNSETTLE_AMOUNT," \
               "REPLACE(REPLACE(COST_MONTH, '年', '-'),'月','') MONTH,NOW() CREATE_TIME," \
               "CONVERT((SUM(COST) + SUM(CASH) + SUM(VOUCHER) + SUM(UNSETTLE_AMOUNT)),DECIMAL (9, 2)) TOTAL  " \
               "FROM asset_cost_record_detail GROUP BY ALI_UUID,ASSET_NO,COST_MONTH) a WHERE a.TOTAL > 0)   "
    cur.execute(save_sql)
    # 将处理过的数据处理状态置为1
    update_sql = " UPDATE asset_cost_record_detail SET IS_HANDLE = 1 WHERE IS_HANDLE = 0 "
    cur.execute(update_sql)


if __name__ == '__main__':
    # dbUser = sys.argv[1]
    # dbPass = sys.argv[2]
    # dbHost = sys.argv[3]
    # dbName = sys.argv[4]
    # file_path = sys.argv[5].decode('gbk').encode('utf8')
    dbUser = 'root'
    dbPass = 'dev123456'
    dbHost = '118.190.21.49'
    dbName = 'cop'
    file_path = 'E:/消费明细/护理教育2017年12月.csv'
    # 读取csv文件并返回拼接好的字符串
    values = file_to_value()
    conn = pymysql.connect(user=dbUser, passwd=dbPass,
                           host=dbHost, db=dbName, use_unicode=True, charset="utf8")
    cur = conn.cursor()
    # 保存数据
    save_detail()
    save_record()

    conn.commit()
    cur.close()
    conn.close()
    print ('执行完成')