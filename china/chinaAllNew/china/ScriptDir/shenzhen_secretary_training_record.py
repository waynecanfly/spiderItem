# -*- coding: utf-8 -*-
import scrapy
import json
import time
from china.items import ChinaIntroItem_str


class CodeSpider(scrapy.Spider):
    name = 'shenzhen_secretary_training_record'
    allowed_domains = ['szse.cn']
    url1_str = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1901&TABKEY=tab2&PAGENO="
    url2_str = "&random=0.5344944650740717"

    def start_requests(self):
            page_str = 1
            url_str = self.url1_str + str(page_str) + self.url2_str
            yield scrapy.Request(
                    url_str,
                    callback=self.parse_str,
                    meta={"page": page_str}
                    )

    def parse_str(self, response):
        page = response.meta["page"]
        data_list = json.loads(response.body)[1]["data"]
        for temp in data_list:
            item = ChinaIntroItem_str()
            item["secretary_name"] = temp["xm"]
            item["exchange_market_code"] = "SZSE"
            item["start_date"] = temp["hddmzgsj"]
            item["recent_training_date"] = temp["zjycpxwcrq"]
            item["gender"] = temp["xb"]
            item["education"] = temp["xl"]
            item["doc_source_url"] = None
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "zx"
            item["spiderName"] = "shenzhen_secretary_training_record"
            yield item
        if len(data_list) != 0:
            page += 1
            url = self.url1_str + str(page) + self.url2_str
            yield scrapy.Request(
                url,
                callback=self.parse_str,
                meta={"page": page}
            )
