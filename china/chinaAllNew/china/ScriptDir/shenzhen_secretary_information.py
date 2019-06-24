# -*- coding: utf-8 -*-
import scrapy
import json
import time
from china.items import ChinaIntroItem_si


class CodeSpider(scrapy.Spider):
    name = 'shenzhen_secretary_information'
    allowed_domains = ['szse.cn']
    url1_si = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1901&TABKEY=tab1&PAGENO="
    url2_si = "&random=0.5344944650740717"

    def start_requests(self):
            page_si = 1
            url_si = self.url1_si + str(page_si) + self.url2_si
            yield scrapy.Request(
                    url_si,
                    callback=self.parse_si,
                    meta={"page": page_si}
                    )

    def parse_si(self, response):
        page = response.meta["page"]
        data_list = json.loads(response.body)[0]["data"]
        for temp in data_list:
            item = ChinaIntroItem_si()
            item["secretary_name"] = temp["xm"]
            item["security_code"] = temp["gsdm"]
            item["exchange_market_code"] = "SZSE"
            item["start_date"] = temp["hddmzgsj"]
            item["recent_training_date"] = temp["zjycpxwcrq"]
            item["gender"] = temp["xb"]
            item["job_title"] = temp["rzqk"]
            item["doc_source_url"] = None
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "zx"
            item["spiderName"] = "shenzhen_secretary_information"
            yield item
        if len(data_list) != 0:
            page += 1
            url = self.url1_si + str(page) + self.url2_si
            yield scrapy.Request(
                url,
                callback=self.parse_si,
                meta={"page": page}
            )
