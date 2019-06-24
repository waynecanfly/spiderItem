# -*- coding: utf-8 -*-
import scrapy
import json
import time
import re
import pymysql
from china.items import ChinaIntroItem_ad
from china.Initialization import Initialization


class CodeSpider(scrapy.Spider):
    name = 'shenzhen_announcement_download'
    allowed_domains = ['szse.cn']
    query_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    pattern_ad = re.compile("\d{4}")
    code_list_ad = []
    report_num_dict_ad = {}
    Keywords_ad = ["报告正文", "已取消", "摘要", "关于"]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql_ad = "select security_code,company_id from company_data_source where company_id like " + "'CHN%'"
    cursor.execute(sql_ad)
    results_ad = cursor.fetchall()

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
            season_num = "07"
        return season_num

    def start_requests(self):
        Initialization().InitializeMain4()
        end_time = str(self.query_time).split(" ")[0]
        for temp in self.results_ad:
            code_ad = temp[0]
            company_id_ad = temp[1]
            if int(code_ad) < 600000:
                self.report_num_dict_ad[company_id_ad] = "000"
                sql_select_ad = "select max(disclosure_date) from financial_statement_index where company_code = %s and" \
                             " exchange_market_code = 'SZSE' and announcement_type = 0 GROUP BY company_code," \
                             "exchange_market_code,announcement_type"
                self.cursor.execute(sql_select_ad, company_id_ad)
                result = self.cursor.fetchone()
                newstdate_ad = int(str(result[0]).replace("-", "").replace(" ", "").replace(":", ""))
                exchange_market_code_ad = "SZSE"
                company_code_ad = company_id_ad
                url_ad = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
                data_ad = {
                    "seDate": ["2008-01-01", end_time],
                    "stock": [code_ad],
                    "channelCode": ["listedNotice_disc"],
                    "bigCategoryId": ["0102", "0107", "0111", "0117", "0105", "0131", "01239901", "01239910", "0139",
                                      "0127", "0115", "0119", "0113", "0121", "0110", "0129", "0109", "0123", "0125"],
                    "pageSize": "30",
                    "pageNum": "1"
                }
                yield scrapy.Request(
                        url_ad,
                        method="POST",
                        body=json.dumps(data_ad),
                        callback=self.parse_ad, meta={
                        "exchange_market_code": exchange_market_code_ad,
                        "company_code": company_code_ad,
                        "newstdate": newstdate_ad
                    })

    def parse_ad(self, response):
        newstdate = response.meta["newstdate"]
        data_list = json.loads(response.body)["data"]
        for temp in data_list:
            item = ChinaIntroItem_ad()
            item["disclosure_date"] = temp["publishTime"]
            # print("%s,%s,%s" % (response.meta["company_code"], newstdate, item["disclosure_date"]))
            if int(str(item["disclosure_date"]).replace("-", "").replace(" ", "").replace(":", "")) > newstdate:
                title = temp["title"]
                item["doc_source_url"] = "http://disc.static.szse.cn/download" + temp["attachPath"]
                item["exchange_market_code"] = response.meta["exchange_market_code"]
                item["company_code"] = response.meta["company_code"]
                item["financial_statement_season_type_code"] = None
                try:
                    item["fiscal_year"] = self.pattern_ad.search(str(title)).group()
                    fiscal_year = item["fiscal_year"]
                except:
                    item["fiscal_year"] = None
                    fiscal_year = "0000"
                num = int(self.report_num_dict_ad[item["company_code"]]) + 1
                num = self.go_heavy_num(num)
                item["report_id"] = item["company_code"] + fiscal_year + "00" + "07" + "01" + num
                self.report_num_dict_ad[item["company_code"]] = num
                item["doc_local_path"] = "/volume1/homes/ChinaNon/" + str(fiscal_year) + "/" + item["report_id"] + ".pdf"
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
                item["spiderName"] = "shenzhen_announcement_download"
                yield item
            else:
                pass
