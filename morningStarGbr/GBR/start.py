# -*- coding: utf-8 -*-
import os
import pymysql
from Initialization import Initialization
from Move_2_Nas import Move2Nas


conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
sql = "delete from gbr_not_parsing"
cursor.execute(sql)
conn.commit()
cursor.close()
conn.close()
print("数据库初始化完成")
os.chdir("/root/spiderItem/morningStarGbr")
Initialization().InitializeMain2()
os.system("python3 /root/spiderItem/morningStarGbr/GBR/fastSpider.py")
os.system("scrapy crawl fileDownload")
try:
    Initialization().InitializeMain()
except FileNotFoundError:
    pass
Move2Nas().Move2NasMain("/data/spiderData/morningStar_gbr", "/homes3/GBR/")
