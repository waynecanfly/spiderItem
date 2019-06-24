# -*- coding: utf-8 -*-
import scrapy
from HCK_l.ScriptDir.Move_2_Nas import Move2Nas
from HCK_l.ScriptDir.Initialization import Initialization


class endPipeline(scrapy.Spider):
    name = 'HCK_ExecuteLast'
    allowed_domains = ['szse.cn']
    start_urls = []

    def start_requests(self):
        Initialization().InitializeMain()
        Move2Nas().Move2NasMain()
        url = "http://www.szse.cn/"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        print("The last step is completed！！！")