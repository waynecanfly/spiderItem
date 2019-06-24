# -*- coding: utf-8 -*-
import scrapy
import time


class CodeSpider(scrapy.Spider):
    name = 'UpdatePro_shanghai'
    allowed_domains = ['sse.com.cn']
    start_urls = ["http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1"]
    create_time_shanghaiL = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    create_time_shanghaiL = str(create_time_shanghaiL).replace("-", "").replace(" ", "").replace(":", "")

    def parse(self, response):
        data = response.text
        with open("/data/OPDCMS/china/listed_company_update/company_list/shanghai_" + self.create_time_shanghaiL + ".txt", "a") as f:
            f.write(data)

