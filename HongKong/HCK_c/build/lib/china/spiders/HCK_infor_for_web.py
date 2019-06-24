# -*- coding: utf-8 -*-
import scrapy
import json
import time
import re
import pymysql
from scrapy_splash import SplashRequest
from china.items import ChinaIntroItem


class CodeSpider(scrapy.Spider):
    # Initialization().InitializeMain2()
    name = 'HCK_infor_for_web'
    allowed_domains = ['hkex']
    type_list = [{"typeCode": "1", "url": "http://www.hkexnews.hk/PrintFriendly/PrintFriendlyUTF.asp?url=http://www.hkexnews.hk/hyperlink/hyperlist.HTM"},
                  {"typeCode": "2", "url": "http://www.hkexnews.hk/PrintFriendly/PrintFriendlyUTF.asp?url=http://www.hkexnews.hk/hyperlink/hyperlist_gem.HTM"}]

    def start_requests(self):
        for temp in self.type_list:
            url = temp["url"]
            typeCode = temp["typeCode"]
            yield scrapy.Request(url, callback=self.parse, meta={"typeCode": typeCode})

    def parse(self, response):
        item = ChinaIntroItem()
        typeCode = response.meta["typeCode"]
        if typeCode == "1":
            tr_list = response.xpath('//table[@class="table_grey_border ms-rteTable-BlueTable_ENG"]//tr[starts-with(@class, "tr_normal")]')
        else:
            tr_list = response.xpath('//table[@class="table_grey_border"]//tr[starts-with(@class, "tr_normal")]')
        for temp in tr_list:
            try:
                value_lenth = temp.xpath('./td[1]/p/text()')
                if len(value_lenth) != 0:
                    item["code"] = "0" + value_lenth.extract()[0]
                else:
                    item["code"] = "0" + temp.xpath('./td[1]/text()').extract()[0]
                item["website_url"] = temp.xpath('./td[3]//a/text()').extract()[0]
                item["doc_source_url"] = None
                yield item
            except Exception as e:
                print(e)