import pymysql


num = 0
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
conn1 = pymysql.connect(host="10.100.4.100", port=3306, db="crawler_india_original", user="root", passwd="originp", charset="utf8")
cursor1 = conn1.cursor()

sql = "select company_id from company_data_source where company_id like 'IND%'"
cursor.execute(sql)
results = cursor.fetchall()
for temp in results:
    sql_select = "select Symbol from crawler_india_original.Basic_Information where company_code = %s"
    cursor1.execute(sql_select, temp[0])
    result = cursor1.fetchone()
    symbol = result[0]
    sql_update = "update company_data_source set info_disclosure_id = %s where company_id = %s"
    params = [
        symbol,
        temp[0]
    ]
    cursor.execute(sql_update, params)
    conn.commit()
    num += 1
    print(num)
conn1.close()
cursor.close()
conn.close()
cursor1.close()
