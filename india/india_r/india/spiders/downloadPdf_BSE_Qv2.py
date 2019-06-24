# -*- coding: utf-8 -*-
import scrapy
import time
import demjson
import re
import pymysql
import random
from datetime import datetime
from india.items import IndiaItem


class CompanylistSpider(scrapy.Spider):
    name = 'downloadPdf_BSE_Qv2'
    allowed_domains = ['bseindia.com']
    nowdate = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    flag = 0
    with open("/root/spiderItem/india/india_r/india/record.txt", "r") as f:
        data = f.read()
    url1 = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strCat=-1&strPrevDate="
    url2 = "&strScrip="
    url3 = "&strSearch=A&strToDate="
    url4 = "&strType=C"
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,company_id from company_data_source where company_id like " + "'IND%' and spider_name = 'BasicInfoBSE'"
    cursor.execute(sql)
    results = cursor.fetchall()
    Q1_list = ["Q1", "First Quarter", "1ST Quarter"]
    Q2_list = ["Q2", "THE QUARTER AND HALF YEAR ENDED", "The Quarter And Half Year Ended", "Half Year Ended",
               "Second Quarter", "2Nd Quarter", "Six Months", "HALF YEAR ENDED", "The Quarter And Half Year Ended",
               "HALF YEAR AND YEAR ENDED"]
    Q3_list = ["Q3", "9 months"]
    Q4_list = ["Q4", "4Th Quarter"]
    Q_list = ["Quarter Ended", "QUARTER ENDED", "Quarter", "June", "Jun", "JUNE", "Qtr. June", "September", "Sep",
              "Sept", "SEPTEMBER", "December", "Dec", "March", "Mar", "MAR", "MARCH", "Three Months Ended",
              "quarter ended"]
    FY_list = ["FY", "Annual", "period ended", "The Quarter And Year Ended", "Quarter And Year Ended", "Year Ended",
               "F.Y", "Financial Year", "FINANCIAL YEAR"]
    Financial_list = ["March", "Mar", "MAR", "MARCH" "Dec", "December", "Sep", "Sept", "September", "SEPTEMBER",
                      "September", "Three Months Ended", "Jun", "JUNE""Qtr. June", "June", "Quarter Ended",
                      "QUARTER ENDED", "Quarter", "Ended", "quarter ended", "Financial Results", "FINANCIAL RESULTS",
                      "Financial Result", "Financial Resuult", "Financial  Results", "FINANCIAL UNAUDITED RESULTS",
                      "Financials Results", "FINANCIAL RESULT", "Financials Result", "FINANCIAL REULTS",
                      "Financial Resut", "Financial Reports", "FINANACIAL RESULT"]

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        if self.flag == 0:
            self.flag += 1
            with open("/root/spiderItem/india/india_r/india/record.txt", "w") as f:
                f.write(str(self.nowdate))
        for temp in self.results:
            code = temp[0]
            company_id = temp[1]
            url = self.url1 + str(self.data).split(" ")[0].replace("-", "") + self.url2 + str(code) + \
                  self.url3 + str(self.nowdate).split(" ")[0].replace("-", "") + self.url4
            yield scrapy.Request(url, callback=self.parse, meta={"company_id": company_id})

    def parse(self, response):
        data_list = re.findall('"NEWSSUB":"(.*?)",.*?"ATTACHMENTNAME":"(.*?)",.*?"DissemDT":"(.*?)",', response.text)
        for temp in data_list:
            item = IndiaItem()
            item["company_code"] = response.meta["company_id"]
            if len(temp[1]) > 3:
                item["doc_source_url"] = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/" + str(temp[1]).replace('"', "")
                title = temp[0]
                item["disclosure_date"] = str(temp[2]).split(".")[0].replace("T", " ")
                item["fiscal_year"] = item["disclosure_date"].split("-")[0]
                if any(i in title for i in self.Q1_list):
                    item["financial_statement_season_type_code"] = "Q1"
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["announcement"] = 1
                elif any(i in title for i in self.Q2_list):
                    item["financial_statement_season_type_code"] = "Q2"
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["announcement"] = 1
                elif any(i in title for i in self.Q3_list):
                    item["financial_statement_season_type_code"] = "Q3"
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["announcement"] = 1
                elif any(i in title for i in self.Q4_list):
                    item["financial_statement_season_type_code"] = "Q4"
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["announcement"] = 1
                elif any(i in title for i in self.Q_list):
                    item["financial_statement_season_type_code"] = "Q"
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["announcement"] = 1
                elif any(i in title for i in self.FY_list):
                    item["financial_statement_season_type_code"] = "FY"
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["announcement"] = 1
                elif any(i in title for i in self.Financial_list):
                    item["financial_statement_season_type_code"] = None
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["announcement"] = 1
                else:
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["announcement"] = 0
                item["country_code"] = "IND"
                item["exchange_market_code"] = "BSE"
                item["financial_reporting_standard_code"] = "IFRS/IND AS"
                item["doc_type"] = "pdf"
                item["is_doc_url_direct"] = 1
                item["is_downloaded"] = 1
                item["currency_code"] = "INR"
                item["doc_downloaded_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["language_written_code"] = "en"
                item["doc_local_path"] = "/volume3/homes3/India/" + item["fiscal_year"] + "/" + item["report_id"] + ".pdf"
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["file_name"] = item["report_id"]
                item["jud"] = 0
                item["pdf_name"] = title
                yield item
