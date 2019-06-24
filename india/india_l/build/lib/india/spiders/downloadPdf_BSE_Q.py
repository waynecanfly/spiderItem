# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
import re
from india.items import IndiaItem
from india.Initialization import Initialization


class CompanylistSpider(scrapy.Spider):
    Initialization().InitializeMain2()
    name = 'downloadPdf_BSE_Q'
    allowed_domains = ['bseindia.com']
    page = 0
    report_num_dict = {}
    pattern = re.compile("xa0")
    url1 = "http://www.bseindia.com/corporates/ann.aspx?curpg=1&annflag=1&dt=&dur=A&dtto=&cat=Result&scrip="
    url2 = "&anntype=C"
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,company_id from company_data_source where mark = 0 and company_id like " + "'IND%' and spider_name = 'BasicInfoBSE'"
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

    def get_pdf_time(self, title):
        """提取pdf中的时间"""
        pattern = re.compile(
            r"Jun\s+?\d+?\s+?\d+|June\s*?\d{2}\s*?\d{4}|JUNE\s*?\d{2}\s*?\d{4}|Sep\s*?\d{2}\s*?\d{4}|Sept\s*?\d{2}\s*?\d{4}|September\s*?\d{2}\s*?\d{4}|Dec\s*?\d{2}\s*?\d{4}|December\s*?\d{2}\s*?\d{4}|March\s*?\d{2}\s*?\d{4}|Mar\s*?\d{2}\s*?\d{4}|MAR\s*?\d{2}\s*?\d{4}|MARCH\s*?\d{2}\s*?\d{4}|SEPTEMBER\s*?\d{2}\s*?\d{4}")
        pdf_time_list = pattern.findall(title)
        if len(pdf_time_list) != 0:
            pdf_time = pdf_time_list[0].replace("June", "06").replace("Jun", "06").replace("JUNE", "06").replace(
                "September", "09").replace("Sept", "09").replace("Sep", "09").replace("SEPTEMBER", "09").replace(
                "December", "12").replace("Dec", "12").replace("March", "03").replace("Mar", "03").replace("MARCH","03").replace(
                "MAR", "03").replace("  ", "").replace(" ", "")
            special_time_list = re.findall("(\d{2})(\d{2})(\d{4})", pdf_time)
            for temp in special_time_list:
                month = temp[0]
                date = int(temp[1])
                if month == "06" or month == "09" and date >= 31:
                    date = 30
                pdf_standard_time = temp[2] + "-" + temp[0] + "-" + str(date) + " 00:00:00"
                end_date = pdf_standard_time
                fiscal_year = str(pdf_standard_time).split("-")[0]
        else:
            end_date = None
            fiscal_year = None
        return end_date, fiscal_year

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
            yield scrapy.Request(url, callback=self.parse, meta={"company_id": company_id, "code": code})

    def parse(self, response):
        company_id = response.meta["company_id"]
        code = response.meta["code"]
        time_list = []
        data_list = response.xpath('//table[@cellspacing="1"]//tr[@style="background-color:white;height:32px;"]')
        for temp in data_list:
            item = IndiaItem()
            item["company_code"] = company_id
            pdf_url = temp.xpath('./preceding-sibling::tr[1]/td[3]/a/@href').extract()
            title = temp.xpath('./preceding-sibling::tr[1]/td[1]/text()').extract()
            title_backup = temp.xpath('./preceding-sibling::tr[1]/td[1]/a/text()').extract()
            data = temp.xpath('./preceding-sibling::tr[2]/td/text()').extract()
            data_time = self.pattern.findall(str(data))
            data_title = self.pattern.findall(str(title))
            if len(pdf_url) == 0:
                continue
            else:
                item["doc_source_url"] = pdf_url[0]
                if len(data_title) != 0 or len(title) == 0:
                    title = str(title_backup[0]).replace("(", "").replace(")", "").replace(",", "").replace("&", "")
                else:
                    title = str(title[0]).replace("(", "").replace(")", "").replace(",", "").replace("&", "")
                if any(i in title for i in self.Q1_list):
                    item["end_date"] = self.get_pdf_time(title)[0]
                    fiscal_year = self.get_pdf_time(title)[1]
                    item["fiscal_year"] = fiscal_year
                    if fiscal_year is None:
                        fiscal_year = "0000"
                    num = int(self.report_num_dict[item["company_code"]]) + 1
                    num = self.go_heavy_num(num)
                    self.report_num_dict[item["company_code"]] = num
                    item["financial_statement_season_type_code"] = "Q1"
                    item["report_id"] = item["company_code"] + fiscal_year + "00" + "01" + "01" + self.report_num_dict[item["company_code"]]
                elif any(i in title for i in self.Q2_list):
                    item["end_date"] = self.get_pdf_time(title)[0]
                    fiscal_year = self.get_pdf_time(title)[1]
                    item["fiscal_year"] = fiscal_year
                    if fiscal_year is None:
                        fiscal_year = "0000"
                    num = int(self.report_num_dict[item["company_code"]]) + 1
                    num = self.go_heavy_num(num)
                    self.report_num_dict[item["company_code"]] = num
                    item["financial_statement_season_type_code"] = "Q2"
                    item["report_id"] = item["company_code"] + fiscal_year + "00" + "02" + "01" + \
                                        self.report_num_dict[item["company_code"]]
                elif any(i in title for i in self.Q3_list):
                    item["end_date"] = self.get_pdf_time(title)[0]
                    fiscal_year = self.get_pdf_time(title)[1]
                    item["fiscal_year"] = fiscal_year
                    if fiscal_year is None:
                        fiscal_year = "0000"
                    num = int(self.report_num_dict[item["company_code"]]) + 1
                    num = self.go_heavy_num(num)
                    self.report_num_dict[item["company_code"]] = num
                    item["financial_statement_season_type_code"] = "Q3"
                    item["report_id"] = item["company_code"] + fiscal_year + "00" + "03" + "01" + \
                                        self.report_num_dict[item["company_code"]]
                elif any(i in title for i in self.Q4_list):
                    item["end_date"] = self.get_pdf_time(title)[0]
                    fiscal_year = self.get_pdf_time(title)[1]
                    item["fiscal_year"] = fiscal_year
                    if fiscal_year is None:
                        fiscal_year = "0000"
                    num = int(self.report_num_dict[item["company_code"]]) + 1
                    num = self.go_heavy_num(num)
                    self.report_num_dict[item["company_code"]] = num
                    item["financial_statement_season_type_code"] = "Q4"
                    item["report_id"] = item["company_code"] + fiscal_year + "00" + "04" + "01" + \
                                        self.report_num_dict[item["company_code"]]
                elif any(i in title for i in self.Q_list):
                    item["end_date"] = self.get_pdf_time(title)[0]
                    fiscal_year = self.get_pdf_time(title)[1]
                    item["fiscal_year"] = fiscal_year
                    if fiscal_year is None:
                        fiscal_year = "0000"
                    num = int(self.report_num_dict[item["company_code"]]) + 1
                    num = self.go_heavy_num(num)
                    self.report_num_dict[item["company_code"]] = num
                    item["financial_statement_season_type_code"] = "Q"
                    item["report_id"] = item["company_code"] + fiscal_year + "00" + "05" + "01" + \
                                        self.report_num_dict[item["company_code"]]
                elif any(i in title for i in self.FY_list):
                    item["end_date"] = self.get_pdf_time(title)[0]
                    fiscal_year = self.get_pdf_time(title)[1]
                    item["fiscal_year"] = fiscal_year
                    if fiscal_year is None:
                        fiscal_year = "0000"
                    num = int(self.report_num_dict[item["company_code"]]) + 1
                    num = self.go_heavy_num(num)
                    self.report_num_dict[item["company_code"]] = num
                    item["financial_statement_season_type_code"] = "FY"
                    item["report_id"] = item["company_code"] + fiscal_year + "00" + "06" + "01" + \
                                        self.report_num_dict[item["company_code"]]
                elif any(i in title for i in self.Financial_list):
                    item["end_date"] = self.get_pdf_time(title)[0]
                    fiscal_year = self.get_pdf_time(title)[1]
                    item["fiscal_year"] = fiscal_year
                    if fiscal_year is None:
                        fiscal_year = "0000"
                    num = int(self.report_num_dict[item["company_code"]]) + 1
                    num = self.go_heavy_num(num)
                    self.report_num_dict[item["company_code"]] = num
                    item["financial_statement_season_type_code"] = None
                    item["report_id"] = item["company_code"] + fiscal_year + "00" + "00" + "01" + \
                                        self.report_num_dict[item["company_code"]]
                else:
                    continue
                if len(data_time) != 0 or len(data) == 0:
                    data = time_list[-1]
                else:
                    data = data[0]
                    time_list.append(data)
                release_time = str(data).replace("Jan", "01").replace("Feb", "02").replace("Mar", "03").replace(
                    "Apr", "04").replace("May", "05").replace("Jun", "06").replace("Jul", "07").replace("Aug","08").replace(
                    "Sep", "09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12").replace("  ","").replace(" ","")
                release_time_list = re.findall("(\d{2})(\d{2})(\d{4})", release_time)
                if len(release_time_list) != 0:
                    temp = release_time_list[0]
                    item["disclosure_date"] = temp[2] + "-" + temp[1] + "-" + temp[0] + " 00:00:00"
                else:
                    item["disclosure_date"] = None
                item["country_code"] = "IND"
                item["exchange_market_code"] = "BSE"
                item["financial_reporting_standard_code"] = "IFRS/IND AS"
                item["doc_type"] = "pdf"
                item["is_doc_url_direct"] = 1
                item["is_downloaded"] = 1
                item["currency_code"] = "INR"
                item["doc_downloaded_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["language_written_code"] = "en"
                item["doc_local_path"] = "/volum1/homes/India/" + fiscal_year + "/" + item["report_id"] + ".pdf"
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "root"
                item["file_name"] = item["report_id"]
                item["jud"] = 0
                yield item
        page_link = response.xpath('//span[@id="ctl00_ContentPlaceHolder1_lblNext"]/a/@href').extract()
        if len(page_link) != 0:
            url = "http://www.bseindia.com/corporates/" + page_link[0]
            yield scrapy.Request(url, callback=self.parse, meta={"code": code, "company_id": company_id})
