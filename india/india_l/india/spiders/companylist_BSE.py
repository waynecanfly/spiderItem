# -*- coding: utf-8 -*-
import scrapy
import time


class CompanylistSpider(scrapy.Spider):
    name = 'companylist_BSE'
    allowed_domains = ['bseindia.com']
    start_urls = ['https://www.bseindia.com/corporates/List_Scrips.aspx?expandable=1']
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    create_time = str(create_time).replace("-", "").replace(" ", "").replace(":", "")

    def parse(self, response):
        __VIEWSTATE = response.xpath('//input[@id="__VIEWSTATE"]/@value').extract()[0]
        __EVENTVALIDATION = response.xpath('//input[@id="__EVENTVALIDATION"]/@value').extract()[0]

        data = {
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": str(__VIEWSTATE),
                "__VIEWSTATEGENERATOR": "CF507786",
                "__EVENTVALIDATION": str(__EVENTVALIDATION),
                "myDestination": "#",
                "WINDOW_NAMER": "1",
                "ctl00$ContentPlaceHolder1$hdnCode": "",
                "ctl00$ContentPlaceHolder1$ddSegment": "Equity",
                "ctl00$ContentPlaceHolder1$ddlStatus": "Select",
                "ctl00$ContentPlaceHolder1$getTExtData": "",
                "ctl00$ContentPlaceHolder1$ddlGroup": "Select",
                "ctl00$ContentPlaceHolder1$ddlIndustry": "Select",
                "ctl00$ContentPlaceHolder1$btnSubmit.x": "22",
                "ctl00$ContentPlaceHolder1$btnSubmit.y": "12"
            }
        yield scrapy.FormRequest.from_response(response, formdata=data, callback=self.aBackResult)

    def aBackResult(self, response):
        __VIEWSTATE = response.xpath('//input[@id="__VIEWSTATE"]/@value').extract()[0]
        __EVENTVALIDATION = response.xpath('//input[@id="__EVENTVALIDATION"]/@value').extract()[0]

        data = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$lnkDownload",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": str(__VIEWSTATE),
            "__VIEWSTATEGENERATOR": "CF507786",
            "__EVENTVALIDATION": str(__EVENTVALIDATION),
            "myDestination": "#",
            "WINDOW_NAMER": "1",
            "ctl00$ContentPlaceHolder1$hdnCode": "",
            "ctl00$ContentPlaceHolder1$ddSegment": "Equity",
            "ctl00$ContentPlaceHolder1$ddlStatus": "Select",
            "ctl00$ContentPlaceHolder1$getTExtData": "",
            "ctl00$ContentPlaceHolder1$ddlGroup": "Select",
            "ctl00$ContentPlaceHolder1$ddlIndustry": "Select",
        }
        yield scrapy.FormRequest.from_response(response, formdata=data, callback=self.bBackResult)

    def bBackResult(self, response):
        data = str(response.text)
        with open("/data/OPDCMS/india/listed_company_update/company_list/BSE_" + self.create_time + ".csv", "a") as f:
             f.write(data)
