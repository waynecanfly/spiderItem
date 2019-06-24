# -*- coding: utf-8 -*-
import scrapy
from germany.ScriptDir.Move_2_Nas import Move2Nas
from germany.ScriptDir.Initialization import Initialization


class endPipeline(scrapy.Spider):
    name = 'germanyExecuteLast'
    allowed_domains = ['baidu.com']
    start_urls = []

    def start_requests(self):
        Initialization().InitializeMain()
        Move2Nas().Move2NasMain()
        url = "https://www.baidu.com/"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        print("The last step is completed！！！")