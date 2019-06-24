# -*- coding: utf-8 -*-
import scrapy
import json
import time
import re
import pymysql
from china.items import ChinaIntroItem_dsn
from china.ScriptDir.Initialization import Initialization


class CodeSpider(scrapy.Spider):
    name = 'shenzhen_download_spider_new'
    allowed_domains = ['szse.cn']
    query_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    pattern_dsn = re.compile("\d.*?\d+")
    report_num_dict_dsn = {}
    report_type_dsn = [{"name": "Q1", "value": "010305"}, {"name": "Q2", "value": "010303"}, {"name": "Q3", "value": "010307"}, {"name": "FY", "value": "010301"}]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql_dsn = "select security_code,company_id from company_data_source where company_id like " + "'CHN%'"
    cursor.execute(sql_dsn)
    results_dsn = cursor.fetchall()

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
        Initialization().InitializeMain2()
        end_time = str(self.query_time).split(" ")[0]
        for temp_dsn in self.results_dsn:
            code_dsn = temp_dsn[0]
            company_id_dsn = temp_dsn[1]
            if int(code_dsn) < 600000:
                self.report_num_dict_dsn[company_id_dsn] = "000"
                for each_dsn in self.report_type_dsn:
                    eachdatelist_dsn = []
                    sql_select_dsn = "select disclosure_date from financial_statement_index where country_code = 'CHN' and company_code = %s and financial_statement_season_type_code = %s"
                    self.cursor.execute(sql_select_dsn, [company_id_dsn, each_dsn["name"]])
                    results_dsn = self.cursor.fetchall()
                    for eachnoe_dsn in results_dsn:
                        eachdate_dsn = int(str(eachnoe_dsn[0]).replace("-", "").replace(" ", "").replace(":", ""))
                        eachdatelist_dsn.append(eachdate_dsn)
                        eachdatelist_dsn.sort()
                    newstdate_dsn = eachdatelist_dsn[-1]
                    exchange_market_code_dsn = "SZSE"
                    company_code_dsn = company_id_dsn
                    financial_statement_season_type_code_dsn = each_dsn["name"]
                    url_dsn = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
                    data_dsn = {
                        "seDate": ["2008-01-01", end_time],
                        "stock": [code_dsn],
                        "channelCode": ["fixed_disc"],
                        "bigCategoryId": [each_dsn["value"]],
                        "pageSize": "30",
                        "pageNum": "1"
                    }
                    yield scrapy.Request(
                            url_dsn,
                            method="POST",
                            body=json.dumps(data_dsn),
                            callback=self.parse_dsn, meta={
                            "exchange_market_code": exchange_market_code_dsn,
                            "company_code": company_code_dsn,
                            "financial_statement_season_type_code": financial_statement_season_type_code_dsn,
                            "newstdate": newstdate_dsn
                            })

    def parse_dsn(self, response):
        newstdate = response.meta["newstdate"]
        data_list = json.loads(response.body)["data"]
        for temp in data_list:
            item = ChinaIntroItem_dsn()
            item["disclosure_date"] = temp["publishTime"]
            if int(str(item["disclosure_date"]).replace("-", "").replace(" ", "").replace(":", "")) > newstdate:
                title = temp["title"]
                item["doc_source_url"] = "http://disc.static.szse.cn/download" + temp["attachPath"]
                item["exchange_market_code"] = response.meta["exchange_market_code"]
                item["company_code"] = response.meta["company_code"]
                item["financial_statement_season_type_code"] = response.meta["financial_statement_season_type_code"]
                try:
                    item["fiscal_year"] = self.pattern_dsn.search(str(title)).group()
                    fiscal_year = item["fiscal_year"]
                except:
                    item["fiscal_year"] = None
                    fiscal_year = "0000"
                season_num = self.jud_season_num(item["financial_statement_season_type_code"])
                num = int(self.report_num_dict_dsn[item["company_code"]]) + 1
                num = self.go_heavy_num(num)
                item["report_id"] = item["company_code"] + fiscal_year + "00" + season_num + "01" + num
                #report_num = re.search("CHN\d{15}(\d{3})", str(item["report_id"])).group(1)
                self.report_num_dict_dsn[item["company_code"]] = num
                #print(item["report_id"])
                item["doc_local_path"] = "/volume1/homes/China/" + str(fiscal_year) + "/" + item["report_id"] + ".pdf"
                item["country_code"] = "CHN"
                item["is_doc_url_direct"] = 1
                item["financial_reporting_standard_code"] = "CAS"
                item["doc_type"] = "pdf"
                item["is_downloaded"] = 1
                item["currency_code"] = "CNY"
                item["language_written_code"] = "zh-simple"
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["doc_downloaded_timestamp"] = item["gmt_create"]
                item["user_create"] = "zx"
                item["file_name"] = title
                item["spiderName"] = "shenzhen_download_spider_new"
                yield item
