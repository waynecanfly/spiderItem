# -*- coding: utf-8 -*-
import scrapy
import time
from italy.items import ItalyCompanyItem, ItalydetailItem


class ItalyallSpider(scrapy.Spider):
    name = 'taiwanlisten'
    allowed_domains = ['twse.com.tw']

    def start_requests(self):
        letterList = [chr(i) for i in range(ord("A"), ord("Z") + 1)]
        for letter in letterList:
            page = 1
            url = "http://emops.twse.com.tw/server-java/t58query"
            data = {
                "step": "0",
                "caption_id": "000001",
                "pagenum": str(page),
                "alphabet": letter,
                "co_id": "",
                "TYPEK": "",
                "industry": ""
            }
            yield scrapy.FormRequest(url, formdata=data, callback=self.parse, meta={"page": page, "letter": letter})

    def parse(self, response):
        letter = response.meta["letter"]
        page = response.meta["page"]
        tr_list = response.xpath('//div[@id="coltable"]/table/tbody/tr')[1:]
        pageNum = response.xpath('//span[@id="pnu"]/a[last()]/text()').extract()[0]
        for temp in tr_list:
            item = ItalyCompanyItem()
            item2 = ItalydetailItem()
            item["name"] = temp.xpath('./td[1]/text()').extract()[0]
            item["security_code"] = temp.xpath('./td[2]/a/text()').extract()[0]
            item2["security_code"] = item["security_code"]
            item["symbol"] = temp.xpath('./td[3]/text()').extract()[0]
            item2["market"] = temp.xpath('./td[4]/text()').extract()[0]
            item["sector"] = temp.xpath('./td[5]/text()').extract()[0]
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "zx"
            item["country_code_listed"] = "TWN"
            item["exchange_market_code"] = "TWSE"
            # yield item
            # yield item2
            print(page, "="*100)
            print(item)
        while page < int(pageNum):
            page += 1
            url = "http://emops.twse.com.tw/server-java/t58query"
            data = {
                "step": "0",
                "caption_id": "000001",
                "pagenum": str(page),
                "alphabet": letter,
                "co_id": "",
                "TYPEK": "",
                "industry": ""
            }
            yield scrapy.FormRequest(url, formdata=data, callback=self.parse, meta={"page": page, "letter": letter})
