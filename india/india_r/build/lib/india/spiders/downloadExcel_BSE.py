# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
import re
from india.items import IndiaItem
from india.Initialization import Initialization


class CompanylistSpider(scrapy.Spider):
    Initialization().InitializeMain2()
    name = 'downloadExcel_BSE'
    allowed_domains = ['bseindia.com']
    report_num_dict = {}
    pattern2 = re.compile(r"IND\d{15}")
    url1 = "http://www.bseindia.com/stock-share-price/stockreach_financials.aspx?scripcode="
    url2 = "&expandable=0"
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,company_id from company_data_source where company_id like " + "'IND%' and spider_name = 'BasicInfoBSE'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def saveExcel(self, tr_list, name):
        for temp in tr_list:
            title = temp.xpath('./td[1]/text()').extract()
            if len(title) == 0:
                title = None
            else:
                title = str(title[0]).replace(",", "|#|")
            value = temp.xpath('./td[2]/text()').extract()
            if len(value) == 0:
                value = ""
            else:
                value = str(value[0]).replace(",", "|#|")
            if title is not None:
                with open("D:\item\OPDCMS/report data update\india\data\csv/" + name + ".csv", "a") as f:
                    f.write(title + "," + value + "\n")

    def go_heavy_num(self, num):
        """获取每家公司的编号"""
        if num < 10:
            num = "00" + str(num)
        elif 10 <= num < 100:
            num = "0" + str(num)
        elif num >= 100:
            num = str(num)
        return num

    def download(self, response):
        report_id_list = response.meta["report_id_list"]
        code = response.meta["code"]
        type = response.meta["type"]
        item = IndiaItem()
        start_year = response.xpath('//table[@id="ctl00_ContentPlaceHolder1_tbl_typeID"]//tr[2]/td[2]/text()').extract()[0]
        end_year = response.xpath('//table[@id="ctl00_ContentPlaceHolder1_tbl_typeID"]//tr[3]/td[2]/text()').extract()[0]
        tr_list = response.xpath('//table[@id="ctl00_ContentPlaceHolder1_tbl_typeID"]//tr')
        start = str(start_year).split("-")
        end = str(end_year).split("-")
        item["start_date"] = "20" + start[-1] + "-" + start[1].replace("Jan", "01").replace("Feb", "02").replace("Mar", "03").replace("Apr",
                    "04").replace("May", "05").replace("Jun", "06").replace("Jul", "07").replace("Aug", "08").replace("Sep",
                    "09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12") + "-" + start[0] + " 00:00:00"
        item["end_date"] = "20" + end[-1] + "-" + end[1].replace("Jan", "01").replace("Feb", "02").replace("Mar", "03").replace("Apr",
                    "04").replace("May", "05").replace("Jun", "06").replace("Jul", "07").replace("Aug", "08").replace("Sep",
                    "09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12") + "-" + end[0] + " 00:00:00"
        item["company_code"] = response.meta["company_id"]
        item["fiscal_year"] = str(item["end_date"]).split("-")[0]
        item["country_code"] = "IND"
        item["exchange_market_code"] = "BSE"
        item["financial_reporting_standard_code"] = "IFRS/IND AS"
        item["doc_type"] = "csv"
        item["source_url"] = response.url
        item["doc_source_url"] = None
        item["is_doc_url_direct"] = 1
        item["is_downloaded"] = 1
        item["currency_code"] = "INR"
        item["doc_downloaded_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item["language_written_code"] = "en"
        num = int(self.report_num_dict[item["company_code"]]) + 1
        num = self.go_heavy_num(num)
        self.report_num_dict[item["company_code"]] = num
        if type == "FY":
            item["report_id"] = item["company_code"] + item["fiscal_year"] + "03" + "06" + "01" + self.report_num_dict[item["company_code"]]
        else:
            item["report_id"] = item["company_code"] + item["fiscal_year"] + "03" + "05" + "01" + self.report_num_dict[item["company_code"]]
        id = self.pattern2.search(str(item["report_id"])).group()
        if id not in report_id_list:
            item["doc_local_path"] = "/volum1/homes/India/" + item["fiscal_year"] + "/" + item["report_id"] + ".csv"
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "root"
            item["financial_statement_type_code"] = "IS"
            del tr_list[0:4]
            self.saveExcel(tr_list, item["report_id"])
            yield item

    def judgument_func(self, request_list):
        type_list = []
        url_list = []
        for temp in request_list:
            type = str(temp).split("=")[-1]
            if type not in type_list:
                type_list.append(type)
        if "c" in type_list:
            for i in request_list:
                type_judgment1 = str(i).split("=")[-1]
                if type_judgment1 == "c":
                    url = i.replace(" ", "+")
                    url_list.append(url)
            type = "C"
        else:
            for j in request_list:
                type_judgment2 = str(j).split("=")[-1]
                if type_judgment2 == "D":
                    url = j.replace(" ", "+")
                    url_list.append(url)
            type = "D"
        del type_list[:]
        return url_list

    def start_requests(self):
        for temp in self.results:
            code = temp[0]
            company_id = temp[1]
            self.report_num_dict[company_id] = "000"
            report_id_list = []
            sql_jud = 'select report_id from financial_statement_index where country_code = "IND" and exchange_market_code = "BSE" and doc_type = "csv" and company_code = %s'
            self.cursor.execute(sql_jud, temp[1])
            results = self.cursor.fetchall()
            for temp in results:
                if temp[0] not in report_id_list:
                    id = self.pattern2.search(temp[0]).group()
                    if id not in report_id_list:
                        report_id_list.append(id)
            url = self.url1 + str(code) + self.url2
            yield scrapy.Request(url, callback=self.parse, meta={"company_id": company_id, "code": code, "report_id_list": report_id_list})

    def parse(self, response):
        report_id_list = response.meta["report_id_list"]
        code = response.meta["code"]
        company_id = response.meta["company_id"]
        quarterly_list = response.xpath('//table[@id="cr"]//tr/td/a/@href').extract()
        annual_list = response.xpath('//table[@id="acr"]//tr/td/a/@href').extract()
        url_list = self.judgument_func(quarterly_list)
        for temp in url_list:
            yield scrapy.Request(temp, callback=self.download, meta={"code": code, "company_id": company_id, "type": "Q", "report_id_list": report_id_list})
        url_list2 = self.judgument_func(annual_list)
        for each in url_list2:
            yield scrapy.Request(each, callback=self.download, meta={"code": code, "company_id": company_id, "type": "FY", "report_id_list": report_id_list})
