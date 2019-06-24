# -*- coding: utf-8 -*-
import scrapy
import requests
import time


class CodeSpider(scrapy.Spider):
    name = 'HCK_Execute1'
    allowed_domains = ['hkex.com.hk']
    start_urls = ["http://www.hkex.com.hk/Products/Securities/Equities?sc_lang=en"]

    def parse(self, response):
        if response.url:
            create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            create_time = str(create_time).replace("-", "").replace(" ", "").replace(":", "")
            Heads = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0'
            }
            xls_link = "http://www.hkex.com.hk" + response.xpath('//div[@class="newsletter"]//li[@class="ls-process-ql__item"]/a/@href').extract()[0]
            req = requests.get(xls_link, headers=Heads)
            with open("/data/OPDCMS/HCK/company_list/HCK_" + create_time + ".xls", "ab") as f:
                f.write(req.content)
