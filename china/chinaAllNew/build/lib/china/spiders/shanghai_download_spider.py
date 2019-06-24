# -*- coding: utf-8 -*-
import scrapy
import time
import re
import pymysql
import json
from scrapy_splash import SplashRequest
from china.items import ChinaIntroItem
from china.Initialization import Initialization


class CodeSpider(scrapy.Spider):
    Initialization().InitializeMain2()
    name = 'shanghai_download_spider'
    allowed_domains = ['sse.com.cn']
    #pattern = re.compile('"result":\[\{.*?"URL":"(.*?(\d{4}-\d{2}-\d{2}).+?\.pdf)",.* ?"title":"(.*?)"')
    eachdatelist = []
    jud_select_list = []
    jud_select_dict = {}
    pattern = re.compile('"result":(.+?]),')
    report_num_dict = {}
    url1 = "http://query.sse.com.cn/infodisplay/queryLatestBulletinNew.do?&jsonCallBack=jsonpCallback6141&productId="
    url2 = "&reportType2=DQGG&reportType="
    url3 = "&beginDate=2017-01-01&endDate=2018-12-31&pageHelp.pageSize=25&pageHelp.pageCount=50&pageHelp.pageNo=1&pageHelp.beginPage=1&pageHelp.cacheSize=1&pageHelp.endPage=5&_=1523513458917"
    report_type = [
        {"name": "Q1", "value": "QUATER1"},
        {"name": "Q2", "value": "QUATER2"},
        {"name": "Q3", "value": "QUATER3"},
        {"name": "FY", "value": "YEARLY"}
    ]
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
            if int(code) >= 600000:
            #if int(code) >= 600827:
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
                    time.sleep(1)
                    item = ChinaIntroItem()
                    item["company_code"] = company_id
                    item["financial_statement_season_type_code"] = each["name"]
                    item["exchange_market_code"] = "SSE"
                    url = self.url1 + code + self.url2 + each["value"] + self.url3
                    yield scrapy.Request(url, callback=self.parse, meta={"item": item, "newstdate": newstdate})

    def parse(self, response):
        newstdate = response.meta["newstdate"]
        item = response.meta["item"]
        html = response.body
        result = self.pattern.search(str(html))
        data = result.group(1)
        data = json.loads(data)
        for temp in data:
            if "摘要" in temp["title"]:
                pass
            else:
                if int(str(temp["SSEDate"]).replace("-", "")) >= newstdate:
                    item["doc_source_url"] = "http://www.sse.com.cn" + temp["URL"]
                    item["fiscal_year"] = temp["bulletin_Year"]
                    item["disclosure_date"] = temp["SSEDate"] + " 00:00:00"
                    item["file_name"] = temp["title"]
                    season_num = self.jud_season_num(item["financial_statement_season_type_code"])
                    num = int(self.report_num_dict[item["company_code"]]) + 1
                    num = self.go_heavy_num(num)
                    item["report_id"] = item["company_code"] + str(item["fiscal_year"]) + "00" + season_num + "01" + num
                    self.report_num_dict[item["company_code"]] = num
                    item["doc_local_path"] = "/volum1/homes/China/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".pdf"
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
                    yield item

"""
                for temp in item:
                    print(temp)
                    print(item[temp])
"""

