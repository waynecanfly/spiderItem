# -*- coding: utf-8 -*-
import scrapy
import time
import re
import pymysql
import json
from scrapy_splash import SplashRequest
from china.items import ChinaIntroItem_sh


class CodeSpider(scrapy.Spider):
    name = 'shanghai_download_spider'
    allowed_domains = ['sse.com.cn']
    eachdatelist_sh = []
    pattern_sh = re.compile('"result":(.+?]),')
    report_num_dict_sh = {}
    url1_sh = "http://query.sse.com.cn/infodisplay/queryLatestBulletinNew.do?&jsonCallBack=jsonpCallback6141&productId="
    url2_sh = "&reportType2=DQGG&reportType="
    url3_sh = "&beginDate=2017-01-01&endDate=2018-12-31&pageHelp.pageSize=25&pageHelp.pageCount=50&pageHelp.pageNo=1&pageHelp.beginPage=1&pageHelp.cacheSize=1&pageHelp.endPage=5&_=1523513458917"
    report_type_sh = [
        {"name": "Q1", "value": "QUATER1"},
        {"name": "Q2", "value": "QUATER2"},
        {"name": "Q3", "value": "QUATER3"},
        {"name": "FY", "value": "YEARLY"}
    ]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql_sh = "select security_code,code from company where code like " + "'CHN%'"
    cursor.execute(sql_sh)
    results_sh = cursor.fetchall()

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
        for temp_sh in self.results_sh:
            code_sh = temp_sh[0]
            company_id_sh = temp_sh[1]
            if int(code_sh) >= 600000:
            #if int(code) >= 600827:
                self.report_num_dict_sh[company_id_sh] = "000"
                for each_sh in self.report_type_sh:
                    eachdatelist_sh = []
                    sql_select_sh = "select disclosure_date from financial_statement_index where country_code = 'CHN' and company_code = %s and financial_statement_season_type_code = %s"
                    self.cursor.execute(sql_select_sh, [company_id_sh, each_sh["name"]])
                    results_sh = self.cursor.fetchall()
                    for eachnoe_sh in results_sh:
                        eachdate_sh = int(str(eachnoe_sh[0]).replace("-", "").replace(" 00:00:00", ""))
                        eachdatelist_sh.append(eachdate_sh)
                        eachdatelist_sh.sort()
                    newstdate_sh = eachdatelist_sh[-1]
                    #print("%s,%s,%s" % (company_id, each["name"], newstdate))
                    time.sleep(1)
                    item = ChinaIntroItem_sh()
                    item["company_code"] = company_id_sh
                    item["financial_statement_season_type_code"] = each_sh["name"]
                    item["exchange_market_code"] = "SSE"
                    url_sh = self.url1_sh + code_sh + self.url2_sh + each_sh["value"] + self.url3_sh
                    yield scrapy.Request(url_sh, callback=self.parse_sh, meta={"item": item, "newstdate": newstdate_sh})

    def parse_sh(self, response):
        newstdate = response.meta["newstdate"]
        item = response.meta["item"]
        html = response.text
        result = self.pattern_sh.search(str(html))
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
                    num = int(self.report_num_dict_sh[item["company_code"]]) + 1
                    num = self.go_heavy_num(num)
                    item["report_id"] = item["company_code"] + str(item["fiscal_year"]) + "00" + season_num + "01" + num
                    self.report_num_dict_sh[item["company_code"]] = num
                    item["doc_local_path"] = "/volume1/homes/China/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".pdf"
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
                    item["spiderName"] = "shanghai_download_spider"
                    yield item
