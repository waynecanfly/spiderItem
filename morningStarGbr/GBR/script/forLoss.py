# -*- coding: utf-8 -*-
import requests
import pymysql
import time
from threading import Thread


def main():
    conn = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select report_id,doc_source_url,id from GBR where is_downloaded =0 and id < 80001 order by id"
    cursor.execute(sql)
    results = cursor.fetchall()
    for temp in results:
        doc_source_url = temp[1]
        jud = str(doc_source_url).split("=")[-1]
        if len(jud) == 9:
            doc_type = ".pdf"
        else:
            doc_type = ".html"
        report_id = temp[0]
        is_downloaded = 1
        gmt_update = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        doc_downloaded_timestamp = gmt_update
        user_update = "zx"
        response = requests.get(doc_source_url, timeout=180)
        data = response.content
        file_name = report_id + doc_type
        try:
            with open("D:/item/ItemData/GBR/" + file_name, "wb") as f:
                f.write(data)
                params = [
                    is_downloaded,
                    gmt_update,
                    doc_downloaded_timestamp,
                    user_update,
                    report_id
                ]
                sql = "update GBR set is_downloaded=%s,gmt_update=%s,doc_downloaded_timestamp=%s,user_update=%s where report_id=%s"
                cursor.execute(sql, params)
                #conn.commit()
                print(temp[2])
        except Exception as e:
            print(e)


t1 = Thread(target=main)
t1.start()
t2 = Thread(target=main)
t2.start()
t3 = Thread(target=main)
t3.start()
t4 = Thread(target=main)
t4.start()
t5 = Thread(target=main)
t5.start()
t6 = Thread(target=main)
t6.start()
# t7 = Thread(target=main)
# t7.start()
# t8 = Thread(target=main)
# t8.start()
