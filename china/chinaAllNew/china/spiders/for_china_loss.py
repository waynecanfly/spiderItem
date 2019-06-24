# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
from china.items import lossItem


class AnnouncementspiderSpider(scrapy.Spider):
    name = 'for_china_loss'
    allowed_domains = ['sse.com.cn', "baidu.com", "szse.cn"]
    start_urls = ["https://www.baidu.com/"]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select report_id,doc_source_url,file_original_title from financial_statement_index where country_code = 'CHN' and doc_type = 'pdf' and is_downloaded = 0"
    cursor.execute(sql)
    results = cursor.fetchall()

    # def start_requests(self):
    #     for temp in self.results:
    #         url = temp[1]
    #         report_id = temp[0]
    #         file_name = temp[2]
    #         yield scrapy.Request(url, callback=self.parse, meta={"report_id": report_id, "file_name": file_name})
    #
    # def parse(self, response):
    #     item = lossItem()
    #     item["doc_source_url"] = response.url
    #     item["doc_type"] = "pdf"
    #     item["report_id"] = response.meta["report_id"]
    #     item["is_downloaded"] = 1
    #     item["gmt_update"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    #     item["doc_downloaded_timestamp"] = item["gmt_update"]
    #     item["user_update"] = "zx"
    #     item["file_name"] = response.meta["file_name"]
    #     yield item

    def parse(self, response):
        for temp in self.results:
            item = lossItem()
            item["doc_source_url"] = temp[1]
            item["doc_type"] = "pdf"
            item["report_id"] = temp[0]
            item["is_downloaded"] = 1
            item["gmt_update"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["doc_downloaded_timestamp"] = item["gmt_update"]
            item["user_update"] = "zx"
            item["file_name"] = temp[2]
            yield item