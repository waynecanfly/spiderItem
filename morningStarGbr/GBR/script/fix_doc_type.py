# -*- coding: utf-8 -*-
import pymysql


conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
sql = "select doc_source_url,doc_type,report_id from financial_statement_index where id > 2732996 and country_code = 'GBR' and user_create = 'zx' "
cursor.execute(sql)
results = cursor.fetchall()

for temp in results:
    jud = str(temp[0]).split("=")[-1]
    if len(jud) == 8 and temp[1] == "html":
        report_id = temp[2]