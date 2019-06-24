# -*- coding: utf-8 -*-
import scrapy
import time
import re
import pymysql
from scrapy_splash import SplashRequest
from china.items import ChinaIntroItem
from china.Initialization import Initialization


class CodeSpider(scrapy.Spider):
    Initialization().InitializeMain2()
    name = 'shenzhen_download_spider'
    allowed_domains = ['sse.com.cn']
    #start_urls = ["http://www.szse.cn/szseWeb/FrontController.szse?randnum=0.577506748569387"]
    pattern = re.compile("\d.*?\d+")
    code_list = []
    report_num_dict = {}
    Keywords = ["报告正文", "已取消", "摘要", "关于"]
    report_type = [{"name": "Q1", "value": "010305"}, {"name": "Q2", "value": "010303"}, {"name": "Q3", "value": "010307"}, {"name": "FY", "value": "010301"}]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,code from company where code like " + "'CHN%'"
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

    def jud_season_num(self, report_type_jud):
        """判断report类型对应编号"""
        if report_type_jud == "Q1":
            season_num = "01"
        elif report_type_jud == "Q2":
            season_num = "02"
        elif report_type_jud == "Q3":
            season_num = "03"
        elif report_type_jud == "FY":
            season_num = "06"
        else:
            season_num = "00"
        return season_num

    def start_requests(self):
        for temp in self.results:
            code = temp[0]
            company_id = temp[1]
            if int(code) < 600000:
                self.report_num_dict[company_id] = "000"
                for each in self.report_type:
                    eachdatelist = []
                    sql_select = "select disclosure_date from financial_statement_index where country_code = 'CHN' and company_code = %s and financial_statement_season_type_code = %s"
                    self.cursor.execute(sql_select, [company_id, each["name"]])
                    results = self.cursor.fetchall()
                    for eachnoe in results:
                        eachdate = int(str(eachnoe[0]).replace("-", "").replace(" 00:00:00", ""))
                        eachdatelist.append(eachdate)
                        eachdatelist.sort()
                    newstdate = eachdatelist[-1]
                    #print("%s,%s,%s" % (company_id, each["name"], newstdate))
                    exchange_market_code = "SZSE"
                    company_code = company_id
                    financial_statement_season_type_code = each["name"]
                    url = "http://disclosure.szse.cn/m/search0425.jsp"
                    data = {
                        "leftid": "1",
                        "lmid": "drgg",
                        "pageNo": "1",
                        "stockCode": str(code),
                        "keyword": "",
                        "noticeType": each["value"],
                        "startTime": "2017-01-01",
                        "endTime": "2018-12-31",
                        "imageField.x": "14",
                        "imageField.y": "17",
                        "tzy": ""
                    }
                    yield scrapy.FormRequest(url, formdata=data, callback=self.parse, meta={
                            "exchange_market_code": exchange_market_code,
                            "company_code": company_code,
                            "financial_statement_season_type_code": financial_statement_season_type_code,
                            "newstdate": newstdate
                            })

    def parse(self, response):
        newstdate = response.meta["newstdate"]
        link_list = response.xpath('//td[@align="left"]//tbody/tr')
        for temp in range(1, len(link_list) + 1):
            item = ChinaIntroItem()
            item["exchange_market_code"] = response.meta["exchange_market_code"]
            item["company_code"] = response.meta["company_code"]
            item["financial_statement_season_type_code"] = response.meta["financial_statement_season_type_code"]
            """
            if item["company_code"] not in self.code_list:
                self.code_list.append(item["company_code"])
                self.num = 0
            """
            title = response.xpath('//td[@align="left"]//tbody/tr' + "[" + str(temp) + "]" + '/td/a/text()').extract()[0]
            if any(i in title for i in self.Keywords):
                pass
            else:
                try:
                    item["fiscal_year"] = self.pattern.search(str(title)).group()
                    fiscal_year = item["fiscal_year"]
                except:
                    item["fiscal_year"] = None
                    fiscal_year = "0000"
                date = response.xpath('//td[@align="left"]//tbody/tr' + "[" + str(temp) + "]" + '/td[@align="left"]/span/text()').extract()[0]
                disclosure_date = str(date).replace("[", "").replace("]", "")
                if int(str(disclosure_date).replace("-", "")) >= newstdate:
                    item["disclosure_date"] = disclosure_date + " 00:00:00"
                    pdf_link = response.xpath('//td[@align="left"]//tbody/tr' + "[" + str(temp) + "]" + '/td[@align="left"]/a/@href').extract()[0]
                    item["doc_source_url"] = "http://disclosure.szse.cn/" + pdf_link
                    season_num = self.jud_season_num(item["financial_statement_season_type_code"])
                    num = int(self.report_num_dict[item["company_code"]]) + 1
                    num = self.go_heavy_num(num)
                    item["report_id"] = item["company_code"] + fiscal_year + "00" + season_num + "01" + num
                    #report_num = re.search("CHN\d{15}(\d{3})", str(item["report_id"])).group(1)
                    self.report_num_dict[item["company_code"]] = num
                    #print(item["report_id"])
                    item["doc_local_path"] = "/volum1/homes/China/" + str(fiscal_year) + "/" + item["report_id"] + ".pdf"
                    item["country_code"] = "CHN"
                    item["financial_reporting_standard_code"] = "CAS"
                    item["doc_type"] = "pdf"
                    item["is_doc_url_direct"] = 1
                    item["is_downloaded"] = 1
                    item["currency_code"] = "CNY"
                    item["language_written_code"] = "zh-simple"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["doc_downloaded_timestamp"] = item["gmt_create"]
                    item["user_create"] = "root"
                    item["file_name"] = title
                    yield item

    """
                    for temp in item:
                        print(temp)
                        print(str(item[temp]).encode("utf-8"))
                    """
