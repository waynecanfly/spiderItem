# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
import random
from datetime import datetime
from italy.items import ItalyfileItem


class ItalyallSpider(scrapy.Spider):
    name = 'info_zhAll'
    allowed_domains = ['twse.com.tw']
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    url_list = [
        {"url": "http://mops.twse.com.tw/mops/web/ajax_t79sb02", "type": "重要子公司基本資料"},
        {"url": "http://mops.twse.com.tw/mops/web/ajax_t79sb03", "type": "重要子公司異動說明"},
        {"url": "http://mops.twse.com.tw/mops/web/ajax_t102sb01", "type": "被投資控股公司基本資料"}
    ]
    sql = "select security_code,company_id from company_data_source where company_id like 'TWN%'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def saveFile(self, name, data):
        with open("/data/spiderData/twn_info/" + name + ".html", "w", encoding="utf-8") as f:
            f.write(data)

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        for temp in self.results:
            company_code = temp[1]
            for each in self.url_list:
                data1 = {
                    "encodeURIComponent": "1",
                    "step": "1",
                    "firstin": "1",
                    "off": "1",
                    "keyword4": "",
                    "code1": "",
                    "TYPEK2": "",
                    "checkbtn": "",
                    "queryName": "co_id",
                    "inpuType": "co_id",
                    "TYPEK": "all",
                    "co_id": str(temp[0])
                }
                yield scrapy.FormRequest(each["url"], formdata=data1, callback=self.parse,
                                         meta={"company_code": company_code, "type": each["type"]})

    def parse(self, response):
        item = ItalyfileItem()
        item["company_code"] = response.meta["company_code"]
        item["report_id"] = item["company_code"] + self.uniqueIDMaker()
        item["disclosure_date"] = None
        item["exchange_market_code"] = "TWSE"
        item["doc_local_path"] = "/volume3/homes3/twnInfo/" + item["report_id"] + ".html"
        item["doc_type"] = "html"
        item["is_downloaded"] = 1
        item["language_written_code"] = "zh-tw"
        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item["user_create"] = "zx"
        item["belong_2_type"] = response.meta["type"]
        self.saveFile(item["report_id"], response.text)
        yield item