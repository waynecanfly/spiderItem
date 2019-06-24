# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
from GBR.items import GbrItem2


class AnnouncementspiderSpider(scrapy.Spider):
    name = 'for_china_loss'
    allowed_domains = ['sse.com.cn', "baidu.com", "szse.cn"]
    start_urls = ["https://www.baidu.com/"]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select report_id,doc_source_url from financial_statement_index where country_code = 'CHN' and doc_type = 'pdf' and is_downloaded = 0"
    cursor.execute(sql)
    results = cursor.fetchall()

    def parse(self, response):
        for temp in self.results:
            item = GbrItem2()
            item["doc_source_url"] = temp[1]
            item["doc_type"] = "pdf"
            item["report_id"] = temp[0]
            item["is_downloaded"] = 1
            item["gmt_update"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["doc_downloaded_timestamp"] = item["gmt_update"]
            item["user_update"] = "zx"
            item["file_name"] = str(item["doc_source_url"]).split("-")[-1].replace(".PDF", "")
            yield item
    # with open("/root/zx/item/GBR/GBR/spiders/lost.txt", "r") as f:
    #     text = f.read()
    #     if text.startswith(u'\ufeff'):
    #         data = text.encode('utf8')[3:].decode('utf8')
    #
    # def parse(self, response):
    #     data_list = self.data.split(",")
    #     for temp in data_list:
    #         sql = "select doc_source_url from financial_statement_index where report_id = %s"
    #         self.cursor.execute(sql, temp)
    #         result = self.cursor.fetchall()
    #         if len(result) != 0:
    #             if result[0][0] != None:
    #                 item = GbrItem2()
    #                 item["doc_source_url"] = result[0]
    #                 item["doc_type"] = "pdf"
    #                 item["report_id"] = temp
    #                 item["is_downloaded"] = 1
    #                 item["gmt_update"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    #                 item["doc_downloaded_timestamp"] = item["gmt_update"]
    #                 item["user_update"] = "zx"
    #                 item["file_name"] = str(item["doc_source_url"]).split("-")[-1].replace(".PDF", "")
    #                 # yield item
    #             else:
    #                 with open("/root/zx/item/GBR/GBR/spiders/lost2.txt", "a") as g:
    #                     g.write(temp + ",")
    #         else:
    #             with open("/root/zx/item/GBR/GBR/spiders/lost2.txt", "a") as g:
    #                 g.write(temp + ",")
