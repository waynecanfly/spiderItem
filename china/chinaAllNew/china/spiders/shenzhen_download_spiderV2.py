# -*- coding: utf-8 -*-
import scrapy
import json
import time
import re
import random
from datetime import datetime
import pymysql
from china.items import ChinaIntroItem_dsn


class CodeSpider(scrapy.Spider):
    name = 'shenzhen_download_spiderV2'
    allowed_domains = ['szse.cn']
    flag = 0
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    end_time = str(nowTime).split(" ")[0]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    pattern_dsn = re.compile("\d.*?\d+")
    report_type_dsn = [{"name": "Q1", "value": "010305"}, {"name": "Q2", "value": "010303"},
                       {"name": "Q3", "value": "010307"}, {"name": "FY", "value": "010301"}]
    f_sql = "select distinct doc_source_url from financial_statement_index where exchange_market_code = 'SZSE' ORDER BY gmt_create desc LIMIT 20000"
    cursor.execute(f_sql)
    f_results = cursor.fetchall()
    f_url = [i[0] for i in f_results]
    with open("/root/spiderItem/china/chinaAllNew/china/record2.txt", "r") as f:
        data = f.read()
        start_time = str(data).split(" ")[0]

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        if self.flag == 0:
            self.flag += 1
            with open("/root/spiderItem/china/chinaAllNew/china/record2.txt", "w") as f:
                f.write(str(self.nowTime))
        for each_dsn in self.report_type_dsn:
            page_dsn = 1
            financial_statement_season_type_code_dsn = each_dsn["name"]
            url_dsn = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
            data_dsn = {
                "seDate": [self.start_time, self.end_time],
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
                    "financial_statement_season_type_code": financial_statement_season_type_code_dsn,
                    "page": page_dsn,
                    "qt": each_dsn["value"]
                })

    def parse_dsn(self, response):
        qt = response.meta["qt"]
        fst = response.meta["financial_statement_season_type_code"]
        page = response.meta["page"]
        data_list = json.loads(response.body)["data"]
        for temp in data_list:
            item = ChinaIntroItem_dsn()
            item["disclosure_date"] = temp["publishTime"]
            item["doc_source_url"] = "http://disc.static.szse.cn/download" + temp["attachPath"]
            if item["doc_source_url"] not in self.f_url:
                title = temp["title"]
                security_code = temp["secCode"][0]
                item["exchange_market_code"] = "SZSE"
                sql = "select code from company where security_code=%s and exchange_market_code= 'SZSE'"
                self.cursor.execute(sql, security_code)
                result = self.cursor.fetchone()
                if result:
                    item["company_code"] = result[0]
                    item["financial_statement_season_type_code"] = fst
                    try:
                        item["fiscal_year"] = self.pattern_dsn.search(str(title)).group()
                        fiscal_year = item["fiscal_year"]
                    except:
                        item["fiscal_year"] = None
                        fiscal_year = "0000"
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["doc_local_path"] = "/volume3/homes3/China/" + str(fiscal_year) + "/" + item["report_id"] + ".pdf"
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
                    item["announcement_type"] = 1
                    yield item
        if len(data_list) > 0:
            page += 1
            url_dsn = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
            data_dsn = {
                "seDate": [self.start_time, self.end_time],
                "channelCode": ["fixed_disc"],
                "bigCategoryId": [qt],
                "pageSize": "30",
                "pageNum": str(page)
            }
            yield scrapy.Request(
                url_dsn,
                method="POST",
                body=json.dumps(data_dsn),
                callback=self.parse_dsn, meta={
                    "financial_statement_season_type_code": fst,
                    "page": page,
                    "qt": qt
                })
