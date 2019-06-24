# -*- coding: utf-8 -*-
import scrapy
from germany.items import GermanyItem


class FrankfurtbasicinfoSpider(scrapy.Spider):
    page = 1
    MaxPage = 1
    name = 'FrankfurtCompanyList'
    allowed_domains = ['en.boerse-frankfurt.de']
    start_urls = ['http://en.boerse-frankfurt.de/search/advancedsharesearchresults?CountryID=1&IsPreferredStock=false&view=financial&p=1&count=25']
    url1 = 'http://en.boerse-frankfurt.de/search/advancedsharesearchresults?CountryID=1&IsPreferredStock=false&view=financial&count=25&p='

    def parse(self, response):
        item = GermanyItem()
        self.MaxPage = response.xpath('//ul[@class="searchlist-submen"]/li[contains(@id,"page")]/a/text()').extract()[-1]
        data_list = response.xpath('//div[@class="table-responsive"]/table[@class="table"]/tbody/tr')
        print(self.MaxPage)
        for temp in data_list:
            Name = temp.xpath('./td[1]//a/strong/text()').extract()
            if len(Name) == 0:
                item["Name"] = "NULL"
            else:
                item["Name"] = Name[0].strip()
            wkn_isin = temp.xpath('./td[1]/div[2]/text()').extract()
            if len(wkn_isin) == 0:
                item["wkn"] = None
                item["isin"] = None
            else:
                item["wkn"] = str(wkn_isin[0]).split("/")[0].strip()
                item["isin"] = str(wkn_isin[0]).split("/")[-1].strip()
            item["doc_source_url"] = None
            yield item
        while self.page < int(self.MaxPage):
            self.page += 1
            url = self.url1 + str(self.page)
            yield scrapy.Request(url, callback=self.parse)
