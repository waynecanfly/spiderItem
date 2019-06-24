# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
"""company表和detail表都有做重复判断"""
import json
import pymysql
import time
from china.items import ChinaIntroItem_shanghaiL
from china.items import ChinaIntroItem_shenzhenL

flag = 0
flag2 = 0
create_time_sz = None
create_time_sh = None


class ChinaCompanyPipeline(object):
    def process_item(self, item, spider):
        if spider.name == "china_updateAll_l":
            global flag, flag2, create_time_sh, create_time_sz
            if isinstance(item, ChinaIntroItem_shenzhenL):
                conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
                cursor = conn.cursor()
                if flag == 0:
                    flag += 1
                    sql_de = "delete from china_plate_type"
                    cursor.execute(sql_de)
                    create_time_sh = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    create_time_sh = str(create_time_sh).replace("-", "").replace(" ", "").replace(":", "")
                f = open('/data/OPDCMS/chinaAll/company_list/shenzhen_' + create_time_sh + ".json", 'a')
                wData = json.dumps(dict(item), ensure_ascii=False) + "@@@"
                f.write(wData)
                params = [
                    item["code"],
                    item["plate_type"],
                    "SZSE",
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql = "insert into china_plate_type(security_code,plate_type,exchange_market_code,gmt_update,user_create)values(%s,%s,%s,%s,%s)"
                cursor.execute(sql, params)
                conn.commit()
                cursor.close()
                conn.close()
            if isinstance(item, ChinaIntroItem_shanghaiL):
                if flag2 == 0:
                    flag2 += 1
                    create_time_sz = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    create_time_sz = str(create_time_sz).replace("-", "").replace(" ", "").replace(":", "")
                with open("/data/OPDCMS/chinaAll/company_list/shanghai_" + create_time_sz + ".txt", "a") as f:
                    f.write(item["data"])
        return item