# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
import re
import random
from datetime import datetime
from india.items import IndiaItem


class CompanylistSpider(scrapy.Spider):
    name = 'downloadExcel_NSE'
    allowed_domains = ['nseindia.com']
    report_num_dict = {}
    pattern2 = re.compile(r"IND\d{15}")
    url1_Last_24_Months = "https://www.nseindia.com/corporates/corpInfo/equities/getFinancialResults.jsp?broadcastPeriod=Last%2024%20Months&symbol="
    url2_Last_24_Months = "&industry=&period="
    url1_More_than_24_Months = "https://www.nseindia.com/corporates/corpInfo/equities/getFinancialResults.jsp?broadcastPeriod=More%20than%2024%20Months&symbol="
    url2_More_than_24_Months = "&industry=&period="
    link1 = "https://www.nseindia.com/corporates/corpInfo/equities/results_new.jsp?param="
    link2 = "&seq_id="
    link3 = "&industry=-&viewFlag=N&frOldNewFlag=N"
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select info_disclosure_id,company_id from company_data_source where company_id like " + "'IND%' and spider_name = 'BasicInfoNSE'"
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
            sql_jud = 'select max(end_date) from financial_statement_index where country_code = "IND" and exchange_market_code = "NSE" and doc_type = "csv" and company_code = %s'
            self.cursor.execute(sql_jud, temp[1])
            result = self.cursor.fetchone()
            if result[0]:
                newstdate = result[0]
            else:
                newstdate = "0"
            url1 = self.url1_Last_24_Months + str(code) + self.url2_Last_24_Months
            # url2 = self.url1_More_than_24_Months + str(code) + self.url2_More_than_24_Months
            yield scrapy.Request(url1, callback=self.parse, meta={"code": code, "company_id": company_id, "newstdate": newstdate})
            # yield scrapy.Request(url2, callback=self.parse, meta={"code": code, "company_id": company_id, "newstdate": newstdate})

    def parse(self, response):
        newstdate = response.meta["newstdate"]
        code = response.meta["code"]
        company_id = response.meta["company_id"]
        data = response.text
        pattern_RelatingTo = re.compile('(?:RelatingTo:".*?")')
        pattern_SeqNumber = re.compile('(?:SeqNumber:".*?")')
        pattern_FromDate = re.compile('(?:FromDate:".*?")')
        pattern_ToDate = re.compile('(?:ToDate:".*?")')
        data_RelatingTo = pattern_RelatingTo.findall(data)
        data_SeqNumber = pattern_SeqNumber.findall(data)
        data_FromDate = pattern_FromDate.findall(data)
        data_ToDate = pattern_ToDate.findall(data)
        for temp in range(len(data_RelatingTo)):
            RelatingTo = data_RelatingTo[temp].split(":")[-1].replace('"', " ").strip()
            SeqNumber = data_SeqNumber[temp].split(":")[-1].replace('"', " ").strip()
            FromDate = data_FromDate[temp].split(":")[-1].replace('"', " ").strip()
            ToDate = data_ToDate[temp].split(":")[-1].replace('"', " ").strip()
            if RelatingTo == "Annual":
                link = self.link1 + FromDate + ToDate + "ANANCNAE" + code + self.link2 + SeqNumber + self.link3
                season_type_code = "FY"
            elif RelatingTo == "First Quarter":
                link = self.link1 + FromDate + ToDate + "Q1UNNNNE" + code + self.link2 + SeqNumber + self.link3
                season_type_code = "Q1"
            elif RelatingTo == "Second Quarter":
                link = self.link1 + FromDate + ToDate + "Q2ANNCNE" + code + self.link2 + SeqNumber + self.link3
                season_type_code = "Q2"
            elif RelatingTo == "Third Quarter":
                link = self.link1 + FromDate + ToDate + "Q3UNNCNE" + code + self.link2 + SeqNumber + self.link3
                season_type_code = "Q3"
            elif RelatingTo == "Fourth Quarter":
                link = self.link1 + FromDate + ToDate + "Q4ANNNAE" + code + self.link2 + SeqNumber + self.link3
                season_type_code = "Q4"
            else:
                link = None
                season_type_code = None
            if link is not None:
                yield scrapy.Request(link, callback=self.getzip, meta={"company_id": company_id,
                                                                       "FromDate": FromDate, "ToDate": ToDate,
                                                                       "season_type_code": season_type_code,
                                                                       "newstdate": newstdate})

    def getzip(self, response):
        newstdate = response.meta["newstdate"]
        item = IndiaItem()
        season_type_code = response.meta["season_type_code"]
        item["company_code"] = response.meta["company_id"]
        start_year = response.meta["FromDate"]
        end_year = response.meta["ToDate"]
        start = str(start_year).split("-")
        end = str(end_year).split("-")
        item["start_date"] = start[-1] + "-" + start[1].replace("Jan", "01").replace("Feb", "02").replace(
            "Mar", "03").replace("Apr", "04").replace("May", "05").replace("Jun", "06").replace("Jul", "07").replace("Aug",
            "08").replace("Sep", "09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12") + "-" + start[0] + " 00:00:00"
        item["end_date"] = end[-1] + "-" + end[1].replace("Jan", "01").replace("Feb", "02").replace("Mar","03").replace(
            "Apr", "04").replace("May", "05").replace("Jun", "06").replace("Jul", "07").replace("Aug", "08").replace(
            "Sep","09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12") + "-" + end[0] + " 00:00:00"
        item["fiscal_year"] = str(item["end_date"]).split("-")[0]
        item["country_code"] = "IND"
        item["exchange_market_code"] = "NSE"
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
        if item["end_date"] > str(newstdate):
            item["doc_local_path"] = "/volume3/homes3/India/" + self.uniqueIDMaker()
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "root"
            item["financial_statement_season_type_code"] = season_type_code
            item["file_name"] = item["report_id"]
            yield item
            tr_list = response.xpath('//td[@valign="top"]//table//tr')
            for temp in tr_list:
                title = temp.xpath('./td[1]//text()').extract()
                if len(title) == 0:
                    title = None
                else:
                    title = str(title[0]).replace("\n", "").replace(",", "|#|")
                value = temp.xpath('./td[2]/text()').extract()
                if len(value) == 0:
                    value = ""
                else:
                    value = str(value[0]).replace("\n", "").replace(",", "|#|")
                if title is not None or title != "\n" or title != " " or title != " ":
                    with open("/data/spiderData/india/" + item["report_id"] + ".csv", "a") as f:
                        f.write(title + "," + value + "\n")
