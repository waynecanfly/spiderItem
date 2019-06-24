# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
import random
from datetime import datetime
from india.items import IndiaItem


class CompanylistSpider(scrapy.Spider):
    name = 'downloadPdf_BSE_A'
    allowed_domains = ['bseindia.com']
    report_num_dict = {}
    url = "https://www.bseindia.com/stock-share-price/financials/annualreports/"
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,company_id from company_data_source where company_id like " + "'IND%' and spider_name = 'BasicInfoBSE'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        for temp in self.results:
            code = temp[0]
            company_id = temp[1]
            sql_jud = 'select max(fiscal_year) from financial_statement_index where country_code = "IND" ' \
                      'and exchange_market_code = "BSE" and doc_type = "pdf" and ' \
                      'financial_statement_season_type_code = "FY" and start_date ' \
                      'is null and end_date is null and company_code = %s'
            self.cursor.execute(sql_jud, temp[1])
            result = self.cursor.fetchone()
            if result[0]:
                max_fiscal_year = result[0]
            else:
                max_fiscal_year = 0
            url = self.url + str(code) + "/"
            yield scrapy.Request(url, callback=self.parse, meta={"company_id": company_id, "max_fiscal_year": max_fiscal_year})

    def parse(self, response):
        max_fiscal_year = response.meta["max_fiscal_year"]
        tr_list = response.xpath('//table[@class="ng-scope"]/tbody/tr')
        for temp in tr_list:
            item = IndiaItem()
            item["fiscal_year"] = temp.xpath('./td[1]/text()').extract()[0]
            if int(item["fiscal_year"]) > int(max_fiscal_year):
                item["doc_source_url"] = "https://www.bseindia.com" + temp.xpath('./td[2]/a/@href').extract()[0]
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
                item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                item["doc_local_path"] = "/volume3/homes3/India/" + item["fiscal_year"] + "/" + item["report_id"] + ".pdf"
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["financial_statement_season_type_code"] = "FY"
                item["file_name"] = item["report_id"]
                item["jud"] = 0
                yield item
