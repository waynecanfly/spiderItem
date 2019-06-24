# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
import re
from india.items import IndiaItem
from india.Initialization import Initialization


class CompanylistSpider(scrapy.Spider):
    Initialization().InitializeMain2()
    name = 'downloadZip_NSE'
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
    sql = "select info_disclosure_id,company_id from company_data_source where mark = 0 and company_id like " + "'IND%' and spider_name = 'BasicInfoNSE'"
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
            self.report_num_dict[company_id] = "100"
            report_id_list = []
            sql_jud = 'select report_id from financial_statement_index where country_code = "IND" and exchange_market_code = "NSE" and doc_type = "pdf" and company_code = %s'
            self.cursor.execute(sql_jud, temp[1])
            results = self.cursor.fetchall()
            for temp in results:
                if temp[0] not in report_id_list:
                    id = self.pattern2.search(temp[0]).group()
                    if id not in report_id_list:
                        report_id_list.append(id)
            url1 = self.url1_Last_24_Months + str(code) + self.url2_Last_24_Months
            url2 = self.url1_More_than_24_Months + str(code) + self.url2_More_than_24_Months
            yield scrapy.Request(url1, callback=self.parse, meta={"code": code, "company_id": company_id, "report_id_list": report_id_list})
            yield scrapy.Request(url2, callback=self.parse, meta={"code": code, "company_id": company_id, "report_id_list": report_id_list})

    def parse(self, response):
        report_id_list = response.meta["report_id_list"]
        code = response.meta["code"]
        company_id = response.meta["company_id"]
        data = response.body
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
                season_num = "06"
            elif RelatingTo == "First Quarter":
                link = self.link1 + FromDate + ToDate + "Q1UNNNNE" + code + self.link2 + SeqNumber + self.link3
                season_type_code = "Q1"
                season_num = "01"
            elif RelatingTo == "Second Quarter":
                link = self.link1 + FromDate + ToDate + "Q2ANNCNE" + code + self.link2 + SeqNumber + self.link3
                season_type_code = "Q2"
                season_num = "02"
            elif RelatingTo == "Third Quarter":
                link = self.link1 + FromDate + ToDate + "Q3UNNCNE" + code + self.link2 + SeqNumber + self.link3
                season_type_code = "Q3"
                season_num = "03"
            elif RelatingTo == "Fourth Quarter":
                link = self.link1 + FromDate + ToDate + "Q4ANNNAE" + code + self.link2 + SeqNumber + self.link3
                season_type_code = "Q4"
                season_num = "04"
            else:
                link = None
                season_type_code = None
                season_num = None
            if link is not None:
                yield scrapy.Request(link, callback=self.getzip,
                                       meta={"company_id": company_id,
                                       "FromDate": FromDate, "ToDate": ToDate,
                                       "season_type_code": season_type_code,
                                       "season_num": season_num, "report_id_list": report_id_list})

    def getzip(self, response):
        report_id_list = response.meta["report_id_list"]
        item = IndiaItem()
        zip_link = response.xpath('//table[@class="viewTable"]//td[@class="t0"]/a/@href').extract()
        if len(zip_link) != 0:
            item["doc_source_url"] = "https://www.nseindia.com" + zip_link[0]
            season_num = response.meta["season_num"]
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
                "Sep", "09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12") + "-" + end[0] + " 00:00:00"
            item["fiscal_year"] = str(item["end_date"]).split("-")[0]
            item["country_code"] = "IND"
            item["exchange_market_code"] = "NSE"
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
            item["report_id"] = item["company_code"] + item["fiscal_year"] + "00" + season_num + "01" + self.report_num_dict[item["company_code"]]
            id = self.pattern2.search(str(item["report_id"])).group()
            if id not in report_id_list:
                item["doc_local_path"] = "/volum1/homes/India/" + item["fiscal_year"] + "/" + item["report_id"] + ".pdf"
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "root"
                item["financial_statement_season_type_code"] = season_type_code
                item["file_name"] = item["report_id"]
                item["jud"] = 1
                yield item
