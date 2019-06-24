# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
import random
from datetime import datetime
from italy.items import ItalyfileItem


class ItalyallSpider(scrapy.Spider):
    name = 'taiwanFileAllv2'
    allowed_domains = ['twse.com.tw']
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

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        for temp in self.results:
            for i1 in self.type_list1:
                for j1 in range(2013, 2019):
                    for k1 in range(1, 5):
                        url1 = i1["url"] + "?TYPEK=all&step=show&co_id=" + temp[0] + "&year=" + str(j1) + "&season=" + str(k1) + "&report_id=C"
                        yield scrapy.Request(url1, callback=self.parse,
                                             meta={"company_code": temp[1], "season": i1["type"],
                                                   "file_name": str(j1) + "_" + str(k1), "fiscal_year": j1})
            for i2 in self.type_list2:
                for j2 in range(2007, 2013):
                    for k2 in range(1, 5):
                        url2 = i2["url"] + "?TYPEK=all&step=show&co_id=" + temp[0] + "&year=" + str(j2) + "&season=" + str(k2)
                        yield scrapy.Request(url2, callback=self.parse, meta={"company_code": temp[1], "season": i2["type"],
                                                                              "file_name": str(j2) + "_" + str(k2), "fiscal_year": j2})
            for i3 in self.type_list3:
                for j3 in range(2007, 2013):
                    url3 = i3["url"] + "?TYPEK=all&step=show&co_id=" + temp[0] + "&year=" + str(j3)
                    yield scrapy.Request(url3, callback=self.parse, meta={"company_code": temp[1], "season": i3["type"],
                                                                          "file_name": str(j3) + "_", "fiscal_year": j3})

    def parse(self, response):
        item = ItalyfileItem()
        item["company_code"] = response.meta["company_code"]
        item["season_type"] = response.meta["season"]
        item["report_id"] = item["company_code"] + self.uniqueIDMaker()
        item["file_name"] = response.meta["file_name"]
        item["fiscal_year"] = response.meta["fiscal_year"]
        item["doc_source_url"] = response.url
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
        yield item
