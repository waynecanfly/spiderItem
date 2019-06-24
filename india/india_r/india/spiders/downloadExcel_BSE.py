# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
import re
import random
from datetime import datetime
from india.items import IndiaItem


class CompanylistSpider(scrapy.Spider):
    name = 'downloadExcel_BSE'
    allowed_domains = ['bseindia.com']
    report_num_dict = {}
    pattern2 = re.compile(r"IND\d{15}")
    url = "https://www.bseindia.com/stock-share-price/financials/results/"
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
                with open("/data/spiderData/india/" + name + ".csv", "a") as f:
                    f.write(title + "," + value + "\n")

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def download(self, response):
        newstdate = response.meta["newstdate"]
        item = IndiaItem()
        try:
            start_year = response.xpath('//table[@id="ContentPlaceHolder1_tbl_typeID"]//tr[2]/td[2]/text()').extract()[0]
            end_year = response.xpath('//table[@id="ContentPlaceHolder1_tbl_typeID"]//tr[3]/td[2]/text()').extract()[0]
            tr_list = response.xpath('//table[@id="ContentPlaceHolder1_tbl_typeID"]//tr')
            start = str(start_year).split("-")
            end = str(end_year).split("-")
            item["start_date"] = "20" + start[-1] + "-" + start[1].replace("Jan", "01").replace("Feb", "02").replace("Mar", "03").replace("Apr",
                        "04").replace("May", "05").replace("Jun", "06").replace("Jul", "07").replace("Aug", "08").replace("Sep",
                        "09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12") + "-" + start[0] + " 00:00:00"
            item["end_date"] = "20" + end[-1] + "-" + end[1].replace("Jan", "01").replace("Feb", "02").replace("Mar", "03").replace("Apr",
                        "04").replace("May", "05").replace("Jun", "06").replace("Jul", "07").replace("Aug", "08").replace("Sep",
                        "09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12") + "-" + end[0] + " 00:00:00"
            if item["end_date"] > str(newstdate):
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
                item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                item["doc_local_path"] = "/volume3/homes3/India/" + item["fiscal_year"] + "/" + item["report_id"] + ".csv"
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["financial_statement_type_code"] = "IS"
                item["financial_statement_season_type_code"] = response.meta["type"]
                del tr_list[0:4]
                self.saveExcel(tr_list, item["report_id"])
                yield item
        except IndexError:
            pass

    def start_requests(self):
        for temp in self.results:
            code = temp[0]
            company_id = temp[1]
            sql_jud = 'select max(end_date) from financial_statement_index where country_code = "IND" and ' \
                      'exchange_market_code = "BSE" and doc_type = "csv" and company_code = %s and is_deleted="0"'
            self.cursor.execute(sql_jud, temp[1])
            result = self.cursor.fetchone()
            if result[0]:
                newstdate = result[0]
            else:
                newstdate = "0"
            url = self.url + str(code) + "/"
            yield scrapy.Request(url, callback=self.parse, meta={"company_id": company_id, "newstdate": newstdate})

    def parse(self, response):
        newstdate = response.meta["newstdate"]
        company_id = response.meta["company_id"]
        quarterly_num = len(response.xpath('//div[@id="qtly"]//table[@class="ng-binding"]//tr[last()-3]/td/text()').extract()) - 1
        annual_num = len(response.xpath('//div[@id="ann"]//table[@class="ng-binding"]//tr[last()-3]/td/text()').extract()) - 1
        # print(quarterly_num, "="*100)
        for temp in range(quarterly_num):
            try:
                url1 = response.xpath('//div[@id="qtly"]//table[@class="ng-binding"]//tr[last()-1]/td[' + str(temp + 2) + ']' + '/a/@href').extract()[0]
            except IndexError:
                url1 = response.xpath('//div[@id="qtly"]//table[@class="ng-binding"]//tr[last()-2]/td[' + str(temp + 2) + ']' + '/a/@href').extract()[0]
            link1 = "https://www.bseindia.com" + url1
            yield scrapy.Request(link1, callback=self.download,
                                 meta={"company_id": company_id, "type": "Q", "newstdate": newstdate})
        for temp in range(annual_num):
            try:
                url2 = response.xpath('//div[@id="ann"]//table[@class="ng-binding"]//tr[last()-1]/td[' + str(temp + 2) + ']' + '/a/@href').extract()[0]
            except IndexError:
                url2 = response.xpath('//div[@id="ann"]//table[@class="ng-binding"]//tr[last()-2]/td[' + str(temp + 2) + ']' + '/a/@href').extract()[0]
            link2 = "https://www.bseindia.com" + url2
            yield scrapy.Request(link2, callback=self.download, meta={"company_id": company_id, "type": "FY",
                                           "newstdate": newstdate})
