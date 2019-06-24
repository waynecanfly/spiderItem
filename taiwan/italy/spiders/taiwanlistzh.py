# -*- coding: utf-8 -*-
import scrapy
import time
from italy.items import ItalyCompanyItem


class ItalyallSpider(scrapy.Spider):
    name = 'taiwanlistzh'
    allowed_domains = ['twse.com.tw']
    url_list = [
        "http://mops.twse.com.tw/mops/web/t123sb06_q1",
        "http://mops.twse.com.tw/mops/web/t123sb06_q2",
        "http://mops.twse.com.tw/mops/web/t123sb06_q3"
    ]

    def start_requests(self):
        for url in self.url_list:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        tr_list = response.xpath('//table[@border="1"]/tbody/tr[@class="even"] | //table[@border="1"]/tbody/tr[@class="odd"]')
        for temp in tr_list:
            item = ItalyCompanyItem()
            industry = temp.xpath('./td[@rowspan="2"][1]/text()').extract()
            if len(industry) != 0:
                item["industry"] = industry[0]
                item["security_code"] = temp.xpath('./td[@rowspan="2"][2]/text()').extract()[0]
                item["name"] = temp.xpath('./td[@rowspan="2"][3]/text()').extract()[0]
                item["status"] = str(temp.xpath('./td[@rowspan="2"][5]/text()').extract()[0]).replace("否", "在售")
                item["country_code_listed"] = "TWN"
                item["exchange_market_code"] = "TWSE"
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                yield item
