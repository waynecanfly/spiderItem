# -*- coding: utf-8 -*-
import scrapy
import pymysql
import time
import random
from datetime import datetime
from australia_l.items import AustraliacompanyupdateItem


class DownloadpdfAsxSpider(scrapy.Spider):
    name = 'DownloadPdf-ASX'
    allowed_domains = ['asx.com.au']
    nowYear = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))).split("-")[0]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA",
                           charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code, company_id from company_data_source where company_id like 'AUS%' and " \
          "download_link = 'https://www.asx.com.au/asx/research/listedCompanies.do' and id > 3890"
    cursor.execute(sql)
    results = cursor.fetchall()
    Q1_list = ["qtr", "quarter1", "q1 financial qtatements"]
    Q2_list = ["half-yearly report and accounts", "appendix 4d", "half result", "interim report",
               "interim financial report", "half yearly report", "half year accounts", "half year",
               "qtrly cashflow statement", "quarterly", "quarter", "half-year", " half yearly",
               "hy", "six months", "december", "half yearly report and accounts",
               " q2 half year financial statements", "interim financial report", "interim report",
               "Interim Review"]
    Q3_list = ["march", "june", "q3 financial statements"]
    Q4_list = ["fourth quarter", "q4 financial statements"]
    Q_list = ["cash flow statement", "appendix 4d", "quarterly qtr", "quarterly",
              "quarterly cashflow report", "appendix4c – quarterly",
              "quarterly report to shareholders", "4c", "quarter ended", "quarter ended",
              "Cash Flow Statement", "Half-yearly report and accounts", "Appendix 4C ,quarterly",
              "Quarterly，appendix 4C"]
    FY_list = ["annual results", "fy2017 annual results", "fy14 =results", "fy10 results",
               "fy17 result", "annual report", "full year financial report",
               "full year financial statements", "preliminary final report", "full year", "ye",
               "4e", "fs & auditor's opinion", "annual financial statements",
               "full year statutory accounts", "appendix 4e", "annual report to shareholders",
               "financial report", "annual financial report", "financial statements", "fy17 result",
               "fy10 results", "fy14 results", "fy2017 annual results", "annual results",
               "fy2015 results", "consolidated financial report"]
    Financial_list = ["preliminary final result", "appendix 4c", "quarter", "quarterly"]

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        for temp in self.results:
            ASX_code = temp[0]
            company_id = temp[1]
            sql_select = "select disclosure_date,doc_source_url from financial_statement_index where disclosure_date " \
                         "in(select max(disclosure_date) as disclosure_date from financial_statement_index where " \
                         "country_code = 'AUS' and exchange_market_code ='ASX' and company_code = %s) and " \
                         "country_code = 'AUS' and exchange_market_code ='ASX' and company_code = %s"
            self.cursor.execute(sql_select, [company_id, company_id])
            results = self.cursor.fetchall()
            if len(results) != 0:
                newstdate = str(results[0][0])
                newsturl = [i[1] for i in results]
            else:
                newstdate = "2007-01-10 00:00:00"
                newsturl = []
            sql_select_non = "select disclosure_date,doc_source_url from non_financial_statement_index where disclosure_date " \
                         "in(select max(disclosure_date) as disclosure_date from non_financial_statement_index where " \
                         "country_code = 'AUS' and exchange_market_code ='ASX' and company_code = %s) and " \
                         "country_code = 'AUS' and exchange_market_code ='ASX' and company_code = %s"
            self.cursor.execute(sql_select_non, [company_id, company_id])
            results_non = self.cursor.fetchall()
            if len(results_non) != 0:
                newstdate_non = str(results_non[0][0])
                newsturl_non = [i[1] for i in results_non]
            else:
                newstdate_non = "2007-01-10 00:00:00"
                newsturl_non = []
            if newstdate <= newstdate_non:
                newstdate = newstdate_non
                newsturl = newsturl_non

            year = int(newstdate.split("-")[0])
            link = "http://www.asx.com.au/asx/statistics/announcements.do?by=asxCode&asxCode=" + str(
                ASX_code) + "&timeframe=Y&year="
            while year <= int(self.nowYear):
                url = link + str(year)
                year += 1
                yield scrapy.Request(url, callback=self.parse,
                                     meta={"company_id": company_id, "newstdate": newstdate, "newsturl": newsturl})

    def parse(self, response):
        newstdate = response.meta["newstdate"]
        newsturl = response.meta["newsturl"]
        company_id = response.meta["company_id"]
        pdf_url_list = response.xpath('//table[@summary="Most recent company announcements"]//tbody/tr')
        for temp in pdf_url_list:
            item = AustraliacompanyupdateItem()
            item["country_code"] = "AUS"
            item["exchange_market_code"] = "ASX"
            item["company_code"] = company_id
            item["financial_reporting_standard_code"] = "AASB"
            item["language_written_code"] = "en"
            item["doc_type"] = "pdf"
            item["is_doc_url_direct"] = 1
            item["doc_downloaded_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["is_downloaded"] = 1
            item["currency_code"] = "AUD"
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "zx"
            link = temp.xpath('./td[3]/a/@href').extract()[0]
            item["original_title"] = str(temp.xpath('./td[3]/a/text()').extract()[0]).replace("\n", "").replace("\t", "")
            pdf_url = "http://www.asx.com.au" + link
            get_time = str(temp.xpath('./td[1]/text()').extract()[0]).replace("\t", "").replace("\n", "").replace("\r", "")
            standard_time_list = get_time.split("/")
            item["fiscal_year"] = standard_time_list[2]
            standard_time = standard_time_list[2] + "-" + standard_time_list[1] + "-" + standard_time_list[0] + " 00:00:00"
            item["disclosure_date"] = standard_time
            title = temp.xpath('./td[3]/a/text()').extract()[0].replace("\t", "").replace("\n", "").replace("\r", "")
            title_lower = str(title).lower()
            if any(i in title_lower for i in self.Q1_list):
                item["financial_statement_season_type_code"] = "Q1"
                item["announcement_type"] = 1
                item["report_id"] = company_id + self.uniqueIDMaker()
            elif any(i in title_lower for i in self.Q2_list):
                item["financial_statement_season_type_code"] = "Q2"
                item["announcement_type"] = 1
                item["report_id"] = company_id + self.uniqueIDMaker()
            elif any(i in title_lower for i in self.Q3_list):
                item["financial_statement_season_type_code"] = "Q3"
                item["announcement_type"] = 1
                item["report_id"] = company_id + self.uniqueIDMaker()
            elif any(i in title_lower for i in self.Q4_list):
                item["financial_statement_season_type_code"] = "Q4"
                item["announcement_type"] = 1
                item["report_id"] = company_id + self.uniqueIDMaker()
            elif any(i in title_lower for i in self.Q_list):
                item["financial_statement_season_type_code"] = "Q"
                item["announcement_type"] = 1
                item["report_id"] = company_id + self.uniqueIDMaker()
            elif any(i in title_lower for i in self.FY_list):
                item["financial_statement_season_type_code"] = "FY"
                item["announcement_type"] = 1
                item["report_id"] = company_id + self.uniqueIDMaker()
            else:
                item["financial_statement_season_type_code"] = ""
                item["announcement_type"] = 0
                item["report_id"] = company_id + self.uniqueIDMaker()
            item["doc_local_path"] = "/volume3/homes3/Australia/" + item["report_id"] + ".pdf"
            item["pdf_name"] = item["report_id"]
            yield scrapy.Request(pdf_url, callback=self.pdf_analyze,
                                 meta={"item": item, "newstdate": newstdate, "newsturl": newsturl})

    def pdf_analyze(self, response):
        newstdate = response.meta["newstdate"]
        newsturl = response.meta["newsturl"]
        item = response.meta["item"]
        value = response.xpath('//form/input[@name="pdfURL"]/@value').extract()[0]
        item["doc_source_url"] = "http://www.asx.com.au" + value
        if item["doc_source_url"] not in newsturl and item["disclosure_date"] >= newstdate:
            yield item
        else:
            pass
