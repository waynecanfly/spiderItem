# # -*- coding: utf-8 -*-
# import scrapy
# import time
# import pymysql
# from GBR.items import GbrItem2
#
#
# class AnnouncementspiderSpider(scrapy.Spider):
#     name = 'for_loss'
#     allowed_domains = ['morningstar.co.uk', "baidu.com"]
#     start_urls = ["https://www.baidu.com/"]
#     conn = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root", passwd="OPDATA", charset="utf8")
#     cursor = conn.cursor()
#     sql = "select report_id,doc_source_url from GBR where is_downloaded =0"
#     cursor.execute(sql)
#     results = cursor.fetchall()
#
#     def parse(self, response):
#         for temp in self.results:
#             item = GbrItem2()
#             item["doc_source_url"] = temp[1]
#             jud = str(item["doc_source_url"]).split("=")[-1]
#             if len(jud) == 9:
#                 item["doc_type"] = "pdf"
#             else:
#                 item["doc_type"] = "html"
#             item["report_id"] = temp[0]
#             item["is_downloaded"] = 1
#             item["gmt_update"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
#             item["doc_downloaded_timestamp"] = item["gmt_update"]
#             item["user_update"] = "zx"
#             item["file_name"] = jud
#             yield item