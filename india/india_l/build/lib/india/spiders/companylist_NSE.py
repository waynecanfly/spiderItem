# -*- coding: utf-8 -*-
import scrapy
import time


class CompanylistSpider(scrapy.Spider):
    name = 'companylist_NSE'
    allowed_domains = ['bseindia.com']
    start_urls = ['https://www.nseindia.com/corporates/datafiles/LDE_EQUITIES_MORE_THAN_5_YEARS.csv']
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    create_time = str(create_time).replace("-", "").replace(" ", "").replace(":", "")

    def parse(self, response):
        data = response.body
        with open("/data/OPDCMS/india/listed_company_update/company_list/NSE_" + self.create_time + ".csv", "ab") as f:
             f.write(data)
