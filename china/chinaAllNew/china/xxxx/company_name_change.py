# -*- coding: utf-8 -*-
import scrapy
import json
import time
from china.items import ChinaIntroItem_shenzhen_nameChange


class CodeSpider(scrapy.Spider):
    name = 'company_name_change'
    allowed_domains = ['szse.cn']
    num_nameChange = 1
    url1_nameChange = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=SSGSGMXX&TABKEY=tab1&PAGENO="
    url2_nameChange = "&random=0.21263260097323644"

    def start_requests(self):
        url_nameChange = self.url1_nameChange + str(self.num_nameChange) + self.url2_nameChange
        yield scrapy.Request(url_nameChange, callback=self.parse_nameChange)

    def parse_nameChange(self, response):
        dataList = json.loads(response.body)[0]["data"]
        if len(dataList) != 0:
            for temp in dataList:
                item = ChinaIntroItem_shenzhen_nameChange()
                item["end_date"] = temp["bgrq"] + " 00:00:00"
                item["code"] = temp["zqdm"]
                item["fomer_name"] = temp["bgqgsqc"]
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["doc_source_url"] = None
                yield item
            self.num_nameChange += 1
            url = self.url1_nameChange + str(self.num_nameChange) + self.url2_nameChange
            yield scrapy.Request(url, callback=self.parse_nameChange)
