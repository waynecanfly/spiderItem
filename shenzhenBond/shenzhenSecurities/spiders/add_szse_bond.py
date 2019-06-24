# -*- coding: utf-8 -*-
import re
import scrapy
import json
import time
from shenzhenSecurities.items import ShenzhensecuritiesItemS


class SecuritiesSpider(scrapy.Spider):
    name = 'add_szse_bond'
    allowed_domains = ['bond.szse.cn']
    page = 1
    start_urls = ["http://bond.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1907_gsz&TABKEY=tab1&PAGENO=1&random=0.33691735290120883"]
    url1 = "http://bond.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1907_gsz&TABKEY=tab1&PAGENO="
    url2 = "&random=0.9736266230245787"

    def parse(self, response):
        data_list = json.loads(response.body)[0]["data"]
        for temp in data_list:
            item = ShenzhensecuritiesItemS()
            item["bond_code"] = re.search("<u>(.*?)</u>", temp["zqdm"]).group(1)
            item['short_name'] = temp["zqjc"]
            item["ipo_date"] = temp["ssrq"]
            item["actual_circulation"] = temp["fxlnew"]
            item["start_interest_date"] = temp["qxrq"]
            item["end_date"] = temp["dqrq"]
            item["coupon_rate"] = temp["pmll"]
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = 'zx'
            yield item
        if len(data_list) == 20:
            self.page += 1
            url = self.url1 + str(self.page) + self.url2
            yield scrapy.Request(url, callback=self.parse)
