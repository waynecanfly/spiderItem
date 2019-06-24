# -*- coding: utf-8 -*-
import scrapy
import pymysql
import re
import time
import random
from datetime import datetime
from germany.items import GermanyItem
from germany.ScriptDir.Initialization import Initialization


class FrankfurtinforSpider(scrapy.Spider):
    Initialization().InitializeMain2()
    name = 'Frankfurtpdf'
    allowed_domains = ['en.boerse-frankfurt.de']
    pattern = re.compile("&datei=(.+)")
    url1 = "http://en.boerse-frankfurt.de/searchresults?_search="
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code, company_id, latest_url from company_data_source where company_id like 'DEU%'"
    cursor.execute(sql)
    results = cursor.fetchall()

    Financial_report_list = ["Appendix 4C", "Quarter", " Quarterly"]
    FY_list = ["Annual financial statements", "Annual Financial Statements", "Annual financial report", "AnnualReport", "Annual Report"]
    Q1_list = ["Q1 statement", "Three-month statement"]
    Q2_list = ["Six-month statement", "Half-yearly financial report", "Half-yearly financial statements", "Half-yearly Report"]
    Q3_list = ["Q3 statement", "Nine-month statement"]

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        for temp in self.results:
            ISIN = temp[0]
            company_id = temp[1]
            latest_mark = temp[2]
            if latest_mark is None:
                latest_mark = "0"
            url = self.url1 + ISIN
            yield scrapy.Request(url, callback=self.parse, meta={"company_id": company_id, "latest_mark": latest_mark})

    def parse(self, response):
        latest_mark = response.meta["latest_mark"]
        link = response.url
        data_list = str(link).split("/")
        data = data_list[-1]
        company_id = response.meta["company_id"]
        url = "http://en.boerse-frankfurt.de/stock/" + "companydata/" + data + "/FSE#CompanyDetails"
        yield scrapy.Request(url, callback=self.detail_func, meta={"company_id": company_id, "latest_mark": latest_mark})

    def detail_func(self, response):
        latest_mark = response.meta["latest_mark"]
        company_id = response.meta["company_id"]
        try:
            link = response.xpath('//div[@id="main-wrapper"]/div[14]//div[@class="col-lg-12"]/h2/a/@href').extract()[-1]
            url = "http://en.boerse-frankfurt.de" + link
            yield scrapy.Request(url, callback=self.pdf_func, meta={"company_id": company_id, "latest_mark": latest_mark})
        except:
            pass

    def pdf_func(self, response):
        latest_mark = response.meta["latest_mark"]
        company_id = response.meta["company_id"]
        data_list = response.xpath('//div[@id="main-wrapper"]/div[5]//div[@class="table-responsive"]/table[@class="table"]/tbody/tr')
        for temp in data_list:
            item = GermanyItem()
            item["doc_source_url"] = temp.xpath('./td/a/@href').extract()[0]
            mark = self.pattern.search(str(item["doc_source_url"]))
            item["latest_mark"] = mark.group(1)
            if int(item["latest_mark"]) > int(latest_mark):
                item["country_code"] = "DEU"
                item["exchange_market_code"] = "Frankfurt"
                item["company_code"] = company_id
                item["financial_reporting_standard_code"] = "IFRS/German GAAP"
                item["language_written_code"] = "de-DE"
                item["doc_type"] = "pdf"
                item["is_doc_url_direct"] = 1
                item["doc_downloaded_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["currency_code"] = "AUD"
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["is_downloaded"] = 1
                date = temp.xpath('./td[1]/text()').extract()[0].replace("\r", "").replace("\n", "").replace("\t", "").replace("/", "-")
                start_time = str(date).split(" till ")[0]
                standard_start_time = start_time.split("-")
                item["start_date"] = standard_start_time[2].strip() + "-" + standard_start_time[1].strip() + "-" + standard_start_time[0].strip() + " 00:00:00"
                end_time = str(date).split(" till ")[1]
                standard_end_time = end_time.split("-")
                item["end_date"] = standard_end_time[2].strip() + "-" + standard_end_time[1].strip() + "-" + standard_end_time[0].strip() + " 00:00:00"
                item["fiscal_year"] = standard_start_time[2]
                title = temp.xpath('./td[2]/text()').extract()[0].replace("\r", "").replace("\n", "").replace("\t", "").replace("/", "-")
                item["origin_pdf_name"] = title
                if any(i in title for i in self.Q1_list):
                    item["financial_statement_season_type_code"] = "Q1"
                    item["announcement_type"] = "1"
                    item["report_id"] = company_id + self.uniqueIDMaker()
                elif any(i in title for i in self.Q2_list):
                    item["financial_statement_season_type_code"] = "Q2"
                    item["announcement_type"] = "1"
                    item["report_id"] = company_id + self.uniqueIDMaker()
                elif any(i in title for i in self.Q3_list):
                    item["financial_statement_season_type_code"] = "Q3"
                    item["announcement_type"] = "1"
                    item["report_id"] = company_id + self.uniqueIDMaker()
                elif any(i in title for i in self.FY_list):
                    item["financial_statement_season_type_code"] = "FY"
                    item["announcement_type"] = "1"
                    item["report_id"] = company_id + self.uniqueIDMaker()
                elif any(i in title for i in self.Financial_report_list):
                    item["financial_statement_season_type_code"] = ""
                    item["announcement_type"] = "1"
                    item["report_id"] = company_id + self.uniqueIDMaker()
                else:
                    item["announcement_type"] = "0"
                    item["report_id"] = company_id + self.uniqueIDMaker()
                item["doc_local_path"] = "/volume3/homes3/Germany/" + item["fiscal_year"] + "/" + item["report_id"] + ".pdf"
                item["pdf_name"] = item["report_id"]
                yield item
            else:
                break
