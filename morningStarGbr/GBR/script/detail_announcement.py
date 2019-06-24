# -*- coding: utf-8 -*-
import pymysql
import json
import time


num = 0
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()

with open("./params.txt", "r") as f:
    text = f.read()
    if text.startswith(u'\ufeff'):
        text = text.encode('utf8')[3:].decode('utf8')
    data = json.loads(text)
    for temp in data:
        name = temp["name"]
        params = [
            name,
            "GBR",
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
            "zx"
        ]
        sql = "insert into Nonfinancial_announcement_detail_type(name,country_code,gmt_create,user_create)value(%s,%s,%s,%s)"
        cursor.execute(sql, params)
        conn.commit()
        num += 1
        print(num)
