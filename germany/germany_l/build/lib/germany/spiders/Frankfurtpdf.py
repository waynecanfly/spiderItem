# -*- coding: utf-8 -*-
import scrapy
import pymysql
import time
from germany.items import GermanyItem
from germany.ScriptDir.Initialization import Initialization


class FrankfurtinforSpider(scrapy.Spider):
    Initialization().InitializeMain2()
    name = 'Frankfurtpdf'
    allowed_domains = ['en.boerse-frankfurt.de']
    url1 = "http://en.boerse-frankfurt.de/searchresults?_search="
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code, company_id from company_data_source where company_id like 'DEU%' and mark = 0"
    cursor.execute(sql)
    results = cursor.fetchall()

    Financial_report_list = ["Appendix 4C", "Quarter", " Quarterly"]
    FY_list = ["Annual financial statements", "Annual Financial Statements", "Annual financial report", "AnnualReport"]
    Q1_list = ["Q1 statement", "Three-month statement"]
    Q2_list = ["Six-month statement", "Half-yearly financial report", "Half-yearly financial statements", "Half-yearly Report"]
    Q3_list = ["Q3 statement", "Nine-month statement"]

    def go_heavy_num(self, num):
            num += 1
            if num < 10:
                num = "00" + str(num)
            elif 10 <= num < 100:
                num = "0" + str(num)
            elif num >= 100:
                num = str(num)
            return num

    def start_requests(self):
        for temp in self.results:
            num = 0
            ISIN = temp[0]
            company_id = temp[1]
            url = self.url1 + ISIN
            yield scrapy.Request(url, callback=self.parse, meta={"company_id": company_id, "num": num})

    def parse(self, response):
        link = response.url
        data_list = str(link).split("/")
        data = data_list[-1]
        num = response.meta["num"]
        company_id = response.meta["company_id"]
        url = "http://en.boerse-frankfurt.de/stock/" + "companydata/" + data + "/FSE#CompanyDetails"
        yield scrapy.Request(url, callback=self.detail_func, meta={"company_id": company_id, "num": num})

    def detail_func(self, response):
        num = response.meta["num"]
        company_id = response.meta["company_id"]
        try:
            link = response.xpath('//div[@id="main-wrapper"]/div[14]//div[@class="col-lg-12"]/h2/a/@href').extract()[-1]
            url = "http://en.boerse-frankfurt.de" + link
            yield scrapy.Request(url, callback=self.pdf_func, meta={"company_id": company_id, "num": num})
        except:
            pass

    def pdf_func(self, response):
        num = response.meta["num"]
        company_id = response.meta["company_id"]
        data_list = response.xpath('//div[@id="main-wrapper"]/div[5]//div[@class="table-responsive"]/table[@class="table"]/tbody/tr')
        for temp in data_list:
            item = GermanyItem()
            item["country_code"] = "DEU"
            item["exchange_market_code"] = "Frankfurt"
            item["company_code"] = company_id
            item["financial_reporting_standard_code"] = "IFRS/German GAAP"
            item["language_written_code"] = "de-DE"
            item["doc_type"] = "pdf"
            item["is_doc_url_direct"] = 1
            item["doc_downloaded_timestamp"] = "20171223000000"
            item["currency_code"] = "AUD"
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "root"
            item["is_downloaded"] = 1
            date = temp.xpath('./td[1]/text()').extract()[0].replace("\r", "").replace("\n", "").replace("\t", "").replace("/", "-")
            start_time = str(date).split(" till ")[0]
            standard_start_time = start_time.split("-")
            item["start_date"] = standard_start_time[2].strip() + "-" + standard_start_time[1].strip() + "-" + standard_start_time[0].strip() + " 00:00:00"
            end_time = str(date).split(" till ")[1]
            standard_end_time = end_time.split("-")
            item["end_date"] = standard_end_time[2].strip() + "-" + standard_end_time[1].strip() + "-" + standard_end_time[0].strip() + " 00:00:00"
            item["fiscal_year"] = standard_start_time[2]
            item["doc_source_url"] = temp.xpath('./td/a/@href').extract()[0]
            title = temp.xpath('./td[2]/text()').extract()[0].replace("\r", "").replace("\n", "").replace("\t", "").replace("/", "-")
            item["origin_pdf_name"] = title
            if any(i in title for i in self.Q1_list):
                num = self.go_heavy_num(num)
                item["financial_statement_season_type_code"] = "Q1"
                item["report_id"] = company_id + item["fiscal_year"] + "00" + "01" + "01" + num
                num = int(num)
            elif any(i in title for i in self.Q2_list):
                num = self.go_heavy_num(num)
                item["financial_statement_season_type_code"] = "Q2"
                item["report_id"] = company_id + item["fiscal_year"] + "00" + "02" + "01" + num
                num = int(num)
            elif any(i in title for i in self.Q3_list):
                num = self.go_heavy_num(num)
                item["financial_statement_season_type_code"] = "Q3"
                item["report_id"] = company_id + item["fiscal_year"] + "00" + "03" + "01" + num
                num = int(num)
            elif any(i in title for i in self.FY_list):
                num = self.go_heavy_num(num)
                item["financial_statement_season_type_code"] = "FY"
                item["report_id"] = company_id + item["fiscal_year"] + "00" + "06" + "01" + num
                num = int(num)
            elif any(i in title for i in self.Financial_report_list):
                num = self.go_heavy_num(num)
                item["financial_statement_season_type_code"] = None
                item["report_id"] = company_id + item["fiscal_year"] + "00" + "00" + "01" + num
                num = int(num)
            else:
                continue
            item["doc_local_path"] = "/volum1/homes/Germany/" + item["fiscal_year"] + "/" + item["report_id"] + ".pdf"
            item["pdf_name"] = item["report_id"]
            yield item
