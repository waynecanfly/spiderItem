# -*- coding: utf-8 -*-
import scrapy
from lxml import etree
import pymysql
import time
from australia_l.items import AustraliacompanyupdateItem


class BasicInfoAsx(scrapy.Spider):
    name = 'BasicInfo_ASX'
    allowed_domains = ['asx.com.au']
    start_urls = []
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code, company_id from company_data_source where company_id like 'AUS%' and " \
          "download_link = 'https://www.asx.com.au/asx/research/listedCompanies.do' and mark = 0"
    cursor.execute(sql)
    results = cursor.fetchall()

    def start_requests(self):
        for temp in self.results:
            item = AustraliacompanyupdateItem()
            management_list = []
            ASX_code = temp[0]
            item["company_id"] = temp[1]
            item["Security_code"] = ASX_code
            link = "http://www.asx.com.au/asx/share-price-research/company/" + str(ASX_code) + "/details"
            yield scrapy.Request(link, callback=self.parse, meta={"management_list": management_list,
                                                                  "item": item})

    def parse(self, response):
        management_list = response.meta["management_list"]
        item = response.meta["item"]
        Title = response.xpath('//div[@class="view-people ng-scope"]/h2/text()').extract()
        if len(Title) != 0:
            item["Title"] = str(Title[0]).replace(" details", "")
        else:
            item["Title"] = None
        description = response.xpath('//div[@class="view-people ng-scope"]/p/text()').extract()
        if len(description) != 0:
            item["Description_title"] = description[0]
        else:
            item["Description_title"] = None
        code = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[1]/td/text()').extract()
        if len(code) != 0:
            item["Issuer_code_title"] = code[0].strip()
        else:
            item["Issuer_code_title"] = None
        x = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[2]/td/text()').extract()
        if len(x) != 0:
            date = str(x[0].strip())
            if date == "Not yet listed":
                item["Official_listing_date"] = None
            else:
                date = date.split("/")
                item["Official_listing_date"] = date[-1] + "-" + date[1] + "-" + date[0] + " 00:00:00"
        else:
            item["Official_listing_date"] = None
        year = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[3]/td/text()').extract()
        if len(year) != 0:
            item["Fiscal_year_title"] = year[0].strip()
        else:
            item["Fiscal_year_title"] = None
        y = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[4]/td/text()').extract()
        if len(y) != 0:
            item["GICS_industry_group_title"] = y[0].strip()
        else:
            item["GICS_industry_group_title"] = None
        z = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[5]/td/text()').extract()
        if len(z) != 0:
            item["Exempt_foreigb_title"] = z[0].strip()
        else:
            item["Exempt_foreigb_title"] = None
        internet_adress = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[6]/td/a/@href').extract()
        if len(internet_adress) != 0:
            item["Internet_address"] = internet_adress[0].strip()
        else:
            item["Internet_address"] = None
        address = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[7]/td/text()').extract()
        if len(address) != 0:
            item["Registered_office_address_title"] = address[0].strip()
        else:
            item["Registered_office_address_title"] = None
        phone = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[8]/td/text()').extract()
        if len(phone) != 0:
            item["Head_office_telephone_title"] = phone[0].strip()
        else:
            item["Head_office_telephone_title"] = None
        fax = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[9]/td/text()').extract()
        if len(fax) != 0:
            item["Head_office_fax_title"] = fax[0].strip()
        else:
            item["Head_office_fax_title"] = None
        registry = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[10]/td/text()').extract()
        if len(registry) != 0:
            item["Share_registry_title"] = registry[0].strip()
        else:
            item["Share_registry_title"] = None
        telephone = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-details"]//tr[11]/td/text()').extract()
        if len(telephone) != 0:
            item["Share_registry_telephone_title"] = telephone[0].strip()
        else:
            item["Share_registry_telephone_title"] = None
        data_list = response.xpath(
            '//div[@class="view-people ng-scope"]//table[@class="table-people company-people"]//tr')
        for temp in data_list:
            name = temp.xpath('./th/text()').extract()[0]
            position = temp.xpath('./td/text()').extract()[0]
            management = name + ": " + position
            management_list.append(management)
        item["Management_title"] = str(management_list)
        message = response.xpath('//div[@class="disclaimer disclaimer-company-info"]/small/text()').extract()
        if len(message) != 0:
            item["Message_title"] = message[0].strip()
        else:
            item["Message_title"] = None
        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item["doc_source_url"] = None
        print(item)
