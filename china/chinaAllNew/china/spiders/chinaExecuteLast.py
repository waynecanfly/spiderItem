# -*- coding: utf-8 -*-
import scrapy
from china.Move_2_Nas import Move2Nas
from china.Initialization import Initialization


class endPipeline(scrapy.Spider):
    name = 'chinaExecuteLast'
    allowed_domains = ['szse.cn']
    start_urls = []

    def start_requests(self):
        Initialization().InitializeMain()
        Move2Nas().Move2NasMain("/data/OPDCMS/chinaAll/pdf", "/homes3/China/")
        Initialization().InitializeMain2()
        url = "http://www.szse.cn/"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        print("The last step is completed！！！")