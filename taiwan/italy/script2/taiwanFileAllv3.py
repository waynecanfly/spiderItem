# -*- coding: utf-8 -*-
import requests
import time
import pymysql
import random
import logging
from lxml import etree
from datetime import datetime


num = 0
logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
formatter = logging.Formatter('%(lineno)d: %(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.FileHandler("fileLog.txt")
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(console)
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
sql = "select security_code,company_id from company_data_source where company_id like 'TWN%'"
cursor.execute(sql)
results = cursor.fetchall()
type_list1 = [
    {"url": "http://emops.twse.com.tw/server-java/t164sb03_e", "type": "1"},
    {"url": "http://emops.twse.com.tw/server-java/t164sb04_e", "type": "2"},
    {"url": "http://emops.twse.com.tw/server-java/t164sb05_e", "type": "3"},
    {"url": "http://emops.twse.com.tw/server-java/t164sb06_e", "type": "4"},
    {"url": "http://emops.twse.com.tw/server-java/t164sb07_e", "type": "5"}
]
type_list2 = [
    {"url": "http://emops.twse.com.tw/server-java/t05st31_e?", "type": "6"},
    {"url": "http://emops.twse.com.tw/server-java/t05st32_e", "type": "7"},
    {"url": "http://emops.twse.com.tw/server-java/t05st35_e", "type": "8"},
    {"url": "http://emops.twse.com.tw/server-java/t05st38_e", "type": "9"},
    {"url": "http://emops.twse.com.tw/server-java/t05st40_e", "type": "10"},
    {"url": "http://emops.twse.com.tw/server-java/t05st33_e", "type": "15"},
    {"url": "http://emops.twse.com.tw/server-java/t05st34_e", "type": "16"},
    {"url": "http://emops.twse.com.tw/server-java/t05st36_e", "type": "17"},
    {"url": "http://emops.twse.com.tw/server-java/t05st39_e", "type": "18"},
    {"url": "http://emops.twse.com.tw/server-java/t05st41_e", "type": "19"}
]
type_list3 = [
    {"url": "http://emops.twse.com.tw/server-java/t05st20_e", "type": "11"},
    {"url": "http://emops.twse.com.tw/server-java/t05st21_e", "type": "12"},
    {"url": "http://emops.twse.com.tw/server-java/t05st29_e", "type": "13"},
    {"url": "http://emops.twse.com.tw/server-java/t05st30_e", "type": "14"}
]


def insertData(item):
    global num
    params = [
        item["country_code"],
        item["exchange_market_code"],
        item["company_code"],
        item["fiscal_year"],
        item["season_type"],
        item["file_name"],
        item["language_written_code"],
        item["doc_type"],
        item["doc_source_url"],
        item["is_doc_url_direct"],
        item["doc_local_path"],
        item["doc_downloaded_timestamp"],
        item["is_downloaded"],
        item["gmt_create"],
        item["user_create"],
        item["report_id"]
    ]
    sql = "insert into financial_statement_index(country_code,exchange_market_code,company_code,fiscal_year," \
          "financial_statement_season_type_code,file_original_title,language_written_code," \
          "doc_type,doc_source_url,is_doc_url_direct,doc_local_path,doc_downloaded_timestamp,is_downloaded," \
          "gmt_create,user_create,report_id)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, params)
    conn.commit()
    num += 1
    print(num)


def saveFile(item):
    try:
        response = requests.get(item["doc_source_url"])
        response.encoding = 'big5'
        data = response.text
        html = etree.HTML(data)
        jud = html.xpath('//table//td')
        if len(jud) > 3:
            time.sleep(1)
            with open("/data/spiderData/taiwan/" + item["report_id"] + ".html", "w", encoding="big5") as f:
                f.write(data)
            insertData(item)
    except Exception as e:
        logger.error(e, item["doc_source_url"])
        pass


def uniqueIDMaker():
    time_id = str(datetime.now()).split(".")[-1]
    random_id1 = str(random.randrange(0, 9))
    random_id2 = str(random.randrange(0, 9))
    unique_id = time_id + random_id1 + random_id2
    return unique_id


def comItem(company_code, season, file_name, fiscal_year, url):
    item = {}
    item["doc_source_url"] = url
    item["company_code"] = company_code
    item["season_type"] = season
    item["report_id"] = item["company_code"] + uniqueIDMaker()
    item["file_name"] = file_name
    item["fiscal_year"] = fiscal_year
    item["exchange_market_code"] = "TWSE"
    item["doc_local_path"] = "/volume3/homes3/TWN/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".html"
    item["country_code"] = "TWN"
    item["doc_type"] = "html"
    item["is_doc_url_direct"] = 1
    item["is_downloaded"] = 1
    item["language_written_code"] = "en"
    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    item["doc_downloaded_timestamp"] = item["gmt_create"]
    item["user_create"] = "zx"
    return item


def start_requests():
    for temp in results:
        for i1 in type_list1:
            for j1 in range(2013, 2019):
                for k1 in range(1, 5):
                    url1 = i1["url"] + "?TYPEK=all&step=show&co_id=" + temp[0] + "&year=" + str(j1) + "&season=" + str(k1) + "&report_id=C"
                    item = comItem(temp[1], i1["type"], str(j1) + "_" + str(k1), j1, url1)
                    saveFile(item)

        for i2 in type_list2:
            for j2 in range(2007, 2013):
                for k2 in range(1, 5):
                    url2 = i2["url"] + "?TYPEK=all&step=show&co_id=" + temp[0] + "&year=" + str(j2) + "&season=" + str(k2)
                    item = comItem(temp[1], i2["type"], str(j2) + "_" + str(k2), j2, url2)
                    saveFile(item)

        for i3 in type_list3:
            for j3 in range(2007, 2013):
                url3 = i3["url"] + "?TYPEK=all&step=show&co_id=" + temp[0] + "&year=" + str(j3)
                item = comItem(temp[1], i3["type"], str(j3) + "_", j3, url3)
                saveFile(item)


if __name__ == "__main__":
    start_requests()