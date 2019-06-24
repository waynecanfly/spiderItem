# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
from HCK_l.items import ChinaIntroItem


class CodeSpider(scrapy.Spider):
    name = 'HCK_pdf_for_loss'
    allowed_domains = ['hkex']
    start_urls = ["http://www.hkex.com.hk"]
    type_list = [".xls", ".XLS"]
    conn = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select report_id,doc_source_url,pdf_name from HCK_pdf where is_downloaded =0"
    cursor.execute(sql)
    results = cursor.fetchall()

    def parse(self, response):
        for temp in self.results:
            item = ChinaIntroItem()
            item["doc_source_url"] = temp[1]
            if any(i in item["doc_source_url"] for i in self.type_list):
                item["doc_type"] = "excel"
            else:
                item["doc_type"] = "pdf"
            item["report_id"] = temp[0]
            item["is_downloaded"] = 1
            item["gmt_update"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["doc_downloaded_timestamp"] = item["gmt_update"]
            item["user_update"] = "zx"
            item["file_name"] = temp[2]
            yield item