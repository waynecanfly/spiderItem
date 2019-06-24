import pymysql
import time

conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA",charset="utf8")
cursor = conn.cursor()

num = 0
sql = "select code, name_origin, security_code from company where country_code_listed =%s"
cursor.execute(sql, "HCK")
results = cursor.fetchall()
for temp in results:
    company_id = temp[0]
    company_name = temp[1]
    security_code = temp[2]
    is_batch = 1
    gmt_create = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    user_create = "zx"
    mark = 1
    spider_name = "HCK_pdf_spider"
    download_link = "http://www.hkexnews.hk/listedco/listconews/advancedsearch/search_active_main.aspx"

    sql_insert = "insert into company_data_source(company_id, company_name, security_code, is_batch, gmt_create, user_create, download_link, mark, spider_name)values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    params = [
        company_id,
        company_name,
        security_code,
        is_batch,
        gmt_create,
        user_create,
        download_link,
        mark,
        spider_name
    ]
    cursor.execute(sql_insert, params)
    conn.commit()
    num += 1
    print(num)
conn.close()
cursor.close()
