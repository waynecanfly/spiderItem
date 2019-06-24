# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
from india.items import IndiaItem
from india.Initialization import Initialization


class CompanylistSpider(scrapy.Spider):
    Initialization().InitializeMain2()
    name = 'downloadPdf_BSE_A'
    allowed_domains = ['bseindia.com']
    report_num_dict = {}
    url1 = "http://www.bseindia.com/stock-share-price/stockreach_annualreports.aspx?scripcode="
    url2 = "&expandable=0"
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,company_id from company_data_source where mark = 0 and company_id like " + "'IND%' and spider_name = 'BasicInfoBSE'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def go_heavy_num(self, num):
        """获取每家公司的编号"""
        if num < 10:
            num = "00" + str(num)
        elif 10 <= num < 100:
            num = "0" + str(num)
        elif num >= 100:
            num = str(num)
        return num

    def start_requests(self):
        for temp in self.results:
            code = temp[0]
            company_id = temp[1]
            self.report_num_dict[company_id] = "000"
            url = self.url1 + str(code) + self.url2
            yield scrapy.Request(url, callback=self.parse, meta={"company_id": company_id})

    def parse(self, response):
        tr_list = response.xpath('//div[@class="content"]//table[@cellspacing="1"]//tr')
        del tr_list[:1]
        for temp in tr_list:
            item = IndiaItem()
            item["fiscal_year"] = temp.xpath('./td[1]/text()').extract()[0]
            item["doc_source_url"] = temp.xpath('./td[2]/a/@href').extract()[0]
            item["company_code"] = response.meta["company_id"]
            item["country_code"] = "IND"
            item["exchange_market_code"] = "BSE"
            item["financial_reporting_standard_code"] = "IFRS/IND AS"
            item["doc_type"] = "pdf"
            item["is_doc_url_direct"] = 1
            item["is_downloaded"] = 1
            item["currency_code"] = "INR"
            item["doc_downloaded_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["language_written_code"] = "en"
            num = int(self.report_num_dict[item["company_code"]]) + 1
            num = self.go_heavy_num(num)
            self.report_num_dict[item["company_code"]] = num
            item["report_id"] = item["company_code"] + item["fiscal_year"] + "00" + "06" + "01" + self.report_num_dict[item["company_code"]]
            item["doc_local_path"] = "/volum1/homes/India/" + item["fiscal_year"] + "/" + item["report_id"] + ".pdf"
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "root"
            item["financial_statement_season_type_code"] = "FY"
            item["file_name"] = item["report_id"]
            item["jud"] = 0
            yield item
