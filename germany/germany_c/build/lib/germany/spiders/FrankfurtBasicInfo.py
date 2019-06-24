# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
from germany.items import GermanyItem


class FrankfurtinforSpider(scrapy.Spider):
    name = 'FrankfurtBasicInfo'
    allowed_domains = ['en.boerse-frankfurt.de']
    url1 = "http://en.boerse-frankfurt.de/searchresults?_search="
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code, company_id from company_data_source where company_id like 'DEU%'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def start_requests(self):
        for temp in self.results:
            item = GermanyItem()
            isin = temp[0]
            item["company_id"] = temp[1]
            item["info_disclosure_id"] = isin
            item["country_code_listed"] = "DEU"
            item["exchange_market_code"] = "Frankfurt"
            item["currency_code"] = "AUD"
            item["doc_source_url"] = None
            url = self.url1 + isin
            yield scrapy.Request(url, callback=self.parse, meta={"item": item})

    def parse(self, response):
        item = response.meta["item"]
        link = response.url
        data_list = str(link).split("/")
        data = data_list[-1]
        item["Transparency_Level_on_First_Quotation"] = response.xpath('//div[@class="row"]/div[1]/div[1]/div[2]//tbody/tr[1]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
        item["Market_Segment"] = response.xpath('//div[@class="row"]/div[1]/div[1]/div[2]//tbody/tr[2]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
        item["Trading_Model"] = response.xpath('//div[@class="row"]/div[1]/div[1]/div[2]//tbody/tr[3]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
        item["Branch"] = response.xpath('//div[@class="row"]/div[1]/div[1]/div[2]//tbody/tr[5]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
        item["Share_Type"] = response.xpath('//div[@class="row"]/div[1]/div[1]/div[2]//tbody/tr[6]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
        item["Sector"] = response.xpath('//div[@class="row"]/div[1]/div[1]/div[2]//tbody/tr[7]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
        item["Subsector"] = response.xpath('//div[@class="row"]/div[1]/div[1]/div[2]//tbody/tr[8]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")

        item["Reuters_Instrument_Code"] = response.xpath('//div[@class="row"]/div[2]/div[1]/div[2]//tbody/tr[2]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
        item["Market_Capitalization"] = response.xpath('//div[@class="row"]/div[2]/div[1]/div[2]//tbody/tr[3]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
        item["Number_of_Shares"] = response.xpath('//div[@class="row"]/div[2]/div[1]/div[2]//tbody/tr[4]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")

        exchange_symbol = item["Exchange_Symbol"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[1]/td[2]/text()')
        if len(exchange_symbol) == 0:
            item["Exchange_Symbol"] = None
            item["CCP_Eligible"] = None
            item["Instrument_Type"] = None
            item["Instrument_Subtype"] = None
            item["Instrument_Group"] = None
            item["Trading_Model_Num"] = None
            item["Min_Tradable_Unit"] = None
            item["Max_Spread"] = None
            item["Min_Quote_Size"] = None
            item["Start_Pre_Trading"] = None
            item["End_Post_Trading"] = None
            item["Start_Intraday_Auction"] = None
        else:
            item["Exchange_Symbol"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[1]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["CCP_Eligible"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[2]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Instrument_Type"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[3]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Instrument_Subtype"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[4]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Instrument_Group"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[5]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Trading_Model_Num"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[6]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Min_Tradable_Unit"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[7]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Max_Spread"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[8]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Min_Quote_Size"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[9]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Start_Pre_Trading"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[10]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["End_Post_Trading"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[11]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Start_Intraday_Auction"] = response.xpath('//div[@class="row"]/div[2]/div[2]/div[2]//tbody/tr[12]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")

        Constituent_of_the_following_Indices = response.xpath('//div[@class="row"]/div[3]/div/div[2]//tbody/tr/td/a/text()')
        if len(Constituent_of_the_following_Indices) == 0:
            item["Constituent_of_the_following_Indices"] = None
        else:
            item["Constituent_of_the_following_Indices"] = Constituent_of_the_following_Indices.extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
        url = "http://en.boerse-frankfurt.de/stock/" + "companydata/" + data + "/FSE#CompanyDetails"
        yield scrapy.Request(url, callback=self.detail_func, meta={"item": item})

    def detail_func(self, response):
        data_list = []
        item = response.meta["item"]
        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item["user_create"] = "zx"
        item["name_origin"] = response.xpath('//div[@class="content"]/span[1]/text()').extract()[0]
        About_the_Company = response.xpath('//div[@id="main-wrapper"]/div[12]//div[@class="box"]/div[2]/p')
        if len(About_the_Company) == 0:
            item["About_the_Company"] = None
        else:
            item["About_the_Company"] = About_the_Company.extract()[0]

        Address = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]//div[1]/div[2]//tr[1]/td[2]/text()')
        if len(Address) == 0:
            item["Address"] = None
            item["Phone"] = None
            item["Fax"] = None
            item["Web"] = None
            item["Contact"] = None
        else:
            item["Address"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]//div[1]/div[2]//tr[1]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Phone"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]//div[1]/div[2]//tr[2]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Fax"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]//div[1]/div[2]//tr[3]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Web"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]//div[1]/div[2]//tr[4]/td[2]/a/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "").replace(">", "")
            item["Contact"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]//div[1]/div[2]//tr[5]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")

        Established = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]/div[2]/div[2]//tr[1]/td[2]/text()')
        if len(Established) == 0:
            item["Established"] = None
            item["Segment"] = None
            item["End_of_Business_Year"] = None
            item["Accounting_Standard"] = None
            item["Registered_Capital"] = None
            item["Admission_Date"] = None
            item["Executive_Board"] = None
            item["Supervisory_Board"] = None
        else:
            item["Established"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]/div[2]/div[2]//tr[1]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Segment"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]/div[2]/div[2]//tr[2]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["End_of_Business_Year"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]/div[2]/div[2]//tr[4]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Accounting_Standard"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]/div[2]/div[2]//tr[5]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Registered_Capital"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]/div[2]/div[2]//tr[6]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Admission_Date"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]/div[2]/div[2]//tr[7]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Executive_Board"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]/div[2]/div[2]//tr[8]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
            item["Supervisory_Board"] = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[1]/div[2]/div[2]//tr[9]/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")

        data = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[2]/div[1]/div[2]//tr')
        if len(data) == 0:
            item["Shareholder_structure_Bearer_shares_without_par"] = None
        else:
            for temp in range(len(data)):
                title = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[2]/div[1]/div[2]//tr/td[1]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
                value = response.xpath('//div[@id="main-wrapper"]/div[13]/div/div[2]/div[1]/div[2]//tr/td[2]/text()').extract()[0].replace("\t", "").replace("\r", "").replace("\n", "")
                title_value = title + ": " + value
                data_list.append(title_value)
            item["Shareholder_structure_Bearer_shares_without_par"] = str(data_list)
        yield item