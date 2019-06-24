# -*- coding: utf-8 -*-
import scrapy
import time
import re
import pymysql
import json
import random
from datetime import datetime, timedelta
from china.items import ChinaIntroItem_sh, ChinaIntroItem_sh_non


class CodeSpider(scrapy.Spider):
    name = 'shanghai_download_spider'
    allowed_domains = ['sse.com.cn']
    pattern_sh = re.compile('"data":(.+?]),')
    nowTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    nowYear = str(nowTime.split(" ")[0]).split("-")[0]
    nowMonth = str(nowTime.split(" ")[0]).split("-")[1]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    report_type_sh = [
        {"qt": "YEARLY", "name": "年报", "detail": "", "ft": "FY"},
        {"qt": "QUATER1", "name": "第一季度季报", "detail": "", "ft": "Q1"},
        {"qt": "QUATER2", "name": "半年报", "detail": "", "ft": "Q2"},
        {"qt": "QUATER3", "name": "第三季度季报", "detail": "", "ft": "Q3"},
        # {"qt": "LSGG", "name": "临时公告", "detail": "253", "ft": ""},
        {"qt": "SHGSZC", "name": "上市公司章程", "detail": "254", "ft": ""},
        {"qt": "FXSSGG", "name": "发行上市公告", "detail": "255", "ft": ""},
        {"qt": "GSZL", "name": "公司治理", "detail": "256", "ft": ""},
        {"qt": "GDDH", "name": "股东大会会议资料", "detail": "257", "ft": ""},
        {"qt": "IPOGG", "name": "IPO公司公告", "detail": "258", "ft": ""},
        {"qt": "QT", "name": "其他", "detail": "259", "ft": ""}
    ]
    url1 = "http://query.sse.com.cn/infodisplay/queryLatestBulletinNew.do?jsonCallBack=jsonpCallback32223&isPagination=true&productId=&keyWord=&reportType2="
    url2 = "&reportType="
    url3 = "&beginDate="
    url4 = "&pageHelp.pageSize=25&pageHelp.pageCount=50&pageHelp.pageNo="
    url5 = "&pageHelp.beginPage="
    url6 = "&pageHelp.cacheSize=1&pageHelp.endPage="
    url7 = "1&_=1541130029769"

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        for each_sh in self.report_type_sh:
            if each_sh["detail"] == "":
                sql_select_sh = "select disclosure_date,doc_source_url from financial_statement_index where " \
                                "disclosure_date in(select max(disclosure_date) as disclosure_date from " \
                                "financial_statement_index where exchange_market_code = 'SSE' and doc_type = 'pdf' " \
                                "and financial_statement_season_type_code = %s) and exchange_market_code = 'SSE' " \
                                "and doc_type = 'pdf' and financial_statement_season_type_code = %s"
                self.cursor.execute(sql_select_sh, [each_sh["ft"], each_sh["ft"]])
                results_sh = self.cursor.fetchall()
                if results_sh[0][0]:
                    newstdate_sh = results_sh[0][0]
                    newsturl_sh = [str(i[1]) for i in results_sh]
                    a = datetime(int(str(newstdate_sh).split(" ")[0].split("-")[0]),
                                 int(str(newstdate_sh).split(" ")[0].split("-")[1]),
                                 int(str(newstdate_sh).split(" ")[0].split("-")[2])
                                 )
                    while a <= datetime.now():
                        flag = 0
                        page = 1
                        b = a + timedelta(days=30)
                        qt_date = str(a).split(" ")[0] + "&endDate=" + str(b).split(" ")[0]
                        a = b + timedelta(days=1)#后期若当天22点程序跑完，新公告22点之后发布，第二天再跑就跳过了当天，可能会漏爬
                        link = self.url1 + "DQGG" + self.url2 + each_sh["qt"] + self.url3 + qt_date + self.url4
                        url = link + str(page) + self.url5 + str(page) + self.url6 + str(page) + self.url7
                        season_type = each_sh["ft"]
                        detail_type = each_sh["detail"]
                        time.sleep(1)
                        yield scrapy.Request(url, callback=self.parse_sh,
                                             meta={"season_type": season_type, "newsturl": newsturl_sh, "newstdate": newstdate_sh,
                                                   "detail_type": detail_type, "link": link, "page": page, "flag": flag})
                else:
                    newstdate_sh = "0"
                    newsturl_sh = []
                    a = datetime(2007, 1, 1)
                    while a <= datetime.now():
                        flag = 0
                        page = 1
                        b = a + timedelta(days=30)
                        qt_date = str(a).split(" ")[0] + "&endDate=" + str(b).split(" ")[0]
                        a = b + timedelta(days=1)
                        link = self.url1 + "DQGG" + self.url2 + each_sh["qt"] + self.url3 + qt_date + self.url4
                        url = link + str(page) + self.url5 + str(page) + self.url6 + str(page) + self.url7
                        season_type = each_sh["ft"]
                        detail_type = each_sh["detail"]
                        time.sleep(1)
                        yield scrapy.Request(url, callback=self.parse_sh,
                                             meta={"season_type": season_type, "newsturl": newsturl_sh, "newstdate": newstdate_sh,
                                                   "detail_type": detail_type, "link": link, "page": page, "flag": flag})
            else:
                sql_select_sh = "select disclosure_date,doc_source_url from non_financial_statement_index where " \
                                "disclosure_date in(select max(disclosure_date) as disclosure from non_financial_statement_index " \
                                "where exchange_market_code = 'SSE' and Non_financial_announcement_detail_type=%s)" \
                                "and exchange_market_code = 'SSE' and Non_financial_announcement_detail_type=%s"
                self.cursor.execute(sql_select_sh, [each_sh["detail"], each_sh["detail"]])
                results_sh = self.cursor.fetchall()
                if results_sh[0][0]:
                    newstdate_sh = results_sh[0][0]
                    newsturl_sh = [str(i[1]) for i in results_sh]
                    a = datetime(int(str(newstdate_sh).split(" ")[0].split("-")[0]),
                                 int(str(newstdate_sh).split(" ")[0].split("-")[1]),
                                 int(str(newstdate_sh).split(" ")[0].split("-")[2])
                                 )
                    while a <= datetime.now():
                        flag = 0
                        page = 1
                        b = a + timedelta(days=30)
                        qt_date = str(a).split(" ")[0] + "&endDate=" + str(b).split(" ")[0]
                        a = b + timedelta(days=1)
                        link = self.url1 + "LSGG" + self.url2 + each_sh["qt"] + self.url3 + qt_date + self.url4
                        url = link + str(page) + self.url5 + str(page) + self.url6 + str(page) + self.url7
                        season_type = each_sh["ft"]
                        detail_type = each_sh["detail"]
                        time.sleep(1)
                        yield scrapy.Request(url, callback=self.parse_sh_non,
                                             meta={"season_type": season_type, "newsturl": newsturl_sh, "newstdate": newstdate_sh,
                                                   "detail_type": detail_type, "link": link, "page": page, "flag": flag})
                else:
                    newstdate_sh = "0"
                    newsturl_sh = []
                    a = datetime(2007, 1, 1)
                    while a <= datetime.now():
                        flag = 0
                        page = 1
                        b = a + timedelta(days=30)
                        qt_date = str(a).split(" ")[0] + "&endDate=" + str(b).split(" ")[0]
                        a = b + timedelta(days=1)
                        link = self.url1 + "LSGG" + self.url2 + each_sh["qt"] + self.url3 + qt_date + self.url4
                        url = link + str(page) + self.url5 + str(page) + self.url6 + str(page) + self.url7
                        season_type = each_sh["ft"]
                        detail_type = each_sh["detail"]
                        time.sleep(1)
                        yield scrapy.Request(url, callback=self.parse_sh_non,
                                             meta={"season_type": season_type, "newsturl": newsturl_sh, "newstdate": newstdate_sh,
                                                   "detail_type": detail_type, "link": link, "page": page, "flag": flag})

    def parse_sh(self, response):
        flag = response.meta["flag"]
        page = response.meta["page"]
        link = response.meta["link"]
        newsturl = response.meta["newsturl"]
        newstdate = response.meta["newstdate"]
        season_type = response.meta["season_type"]
        detail_type = response.meta["detail_type"]
        html = response.text
        result = self.pattern_sh.search(str(html))
        data = result.group(1)
        data = json.loads(data)
        for temp in data:
            item = ChinaIntroItem_sh()
            security_code = temp["security_Code"]
            sql = "select code from company where security_code=%s and exchange_market_code= 'SSE'"
            self.cursor.execute(sql, security_code)
            result = self.cursor.fetchone()
            if result:
                item["company_code"] = result[0]
                item["financial_statement_season_type_code"] = season_type
                # if season_type == "":
                #     item["announcement_type"] = 0
                # else:
                #     item["announcement_type"] = 1
                item["announcement_type"] = 1
                item["exchange_market_code"] = "SSE"
                item["doc_source_url"] = "http://www.sse.com.cn" + temp["URL"]
                item["disclosure_date"] = temp["SSEDate"] + " 00:00:00"
                if item["doc_source_url"] in newsturl or item["disclosure_date"] < str(newstdate):
                    flag += 1
                    break
                else:
                    item["fiscal_year"] = temp["bulletin_Year"]
                    item["file_name"] = temp["title"]
                    # item["detail_type"] = detail_type
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["doc_local_path"] = "/volume3/homes3/China/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".pdf"
                    item["country_code"] = "CHN"
                    item["financial_reporting_standard_code"] = "CAS"
                    item["doc_type"] = "pdf"
                    item["is_doc_url_direct"] = 1
                    item["is_downloaded"] = 1
                    item["currency_code"] = "CNY"
                    item["language_written_code"] = "zh-simple"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["doc_downloaded_timestamp"] = item["gmt_create"]
                    item["user_create"] = "zx"
                    item["spiderName"] = "shanghai_download_spider"
                    sql = "select id from financial_statement_index where exchange_market_code = 'SSE' and doc_source_url=%s"
                    self.cursor.execute(sql, item["doc_source_url"])
                    result_id = self.cursor.fetchone()
                    if result_id[0]:
                        print(result_id, "+" * 100)
                        pass
                    else:
                        print(result_id, "=" * 100)
                        # yield item
        if flag == 0:
            if len(data) > 0:
                page += 1
                url = link + str(page) + self.url5 + str(page) + self.url6 + str(page) + self.url7
                yield scrapy.Request(url, callback=self.parse_sh,
                                     meta={"season_type": season_type, "newsturl": newsturl, "newstdate": newstdate,
                                           "detail_type": detail_type, "link": link, "page": page, "flag": flag})

    def parse_sh_non(self, response):
        flag = response.meta["flag"]
        page = response.meta["page"]
        link = response.meta["link"]
        newsturl = response.meta["newsturl"]
        newstdate = response.meta["newstdate"]
        season_type = response.meta["season_type"]
        detail_type = response.meta["detail_type"]
        html = response.text
        result = self.pattern_sh.search(str(html))
        data = result.group(1)
        data = json.loads(data)
        for temp in data:
            item = ChinaIntroItem_sh_non()
            security_code = temp["security_Code"]
            sql = "select code from company where security_code=%s and exchange_market_code= 'SSE'"
            self.cursor.execute(sql, security_code)
            result = self.cursor.fetchone()
            if result:
                item["company_code"] = result[0]
                item["financial_statement_season_type_code"] = season_type
                # if season_type == "":
                #     item["announcement_type"] = 0
                # else:
                #     item["announcement_type"] = 1
                item["exchange_market_code"] = "SSE"
                item["doc_source_url"] = "http://www.sse.com.cn" + temp["URL"]
                item["disclosure_date"] = temp["SSEDate"] + " 00:00:00"
                if item["doc_source_url"] in newsturl or item["disclosure_date"] < str(newstdate):
                    flag += 1
                    break
                else:
                    item["fiscal_year"] = temp["bulletin_Year"]
                    item["file_name"] = temp["title"]
                    item["detail_type"] = detail_type
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["doc_local_path"] = "/volume3/homes3/China/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".pdf"
                    item["country_code"] = "CHN"
                    # item["financial_reporting_standard_code"] = "CAS"
                    item["doc_type"] = "pdf"
                    item["is_doc_url_direct"] = 1
                    item["is_downloaded"] = 1
                    # item["currency_code"] = "CNY"
                    item["language_written_code"] = "zh-simple"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["doc_downloaded_timestamp"] = item["gmt_create"]
                    item["user_create"] = "zx"
                    item["spiderName"] = "shanghai_download_spider"
                    sql = "select id from non_financial_statement_index where exchange_market_code = 'SSE' and doc_source_url=%s"
                    self.cursor.execute(sql, item["doc_source_url"])
                    result_id = self.cursor.fetchone()
                    if result_id:
                        print(result_id, "+" * 100)
                        pass
                    else:
                        print(result_id, "=" * 100)
                        # yield item
        if flag == 0:
            if len(data) > 0:
                page += 1
                url = link + str(page) + self.url5 + str(page) + self.url6 + str(page) + self.url7
                yield scrapy.Request(url, callback=self.parse_sh_non,
                                     meta={"season_type": season_type, "newsturl": newsturl, "newstdate": newstdate,
                                           "detail_type": detail_type, "link": link, "page": page, "flag": flag})
