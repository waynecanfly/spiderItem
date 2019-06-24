# -*- coding: utf-8 -*-
import scrapy
import time
import json
import pymysql
import datetime
import random
import re
from shenzhenSecurities.items import ShenzhensecuritiesItem, ShenzhensecuritiesItemOther


class SecuritiesSpider(scrapy.Spider):
    name = 'fileProUpdate'
    allowed_domains = ['szse.cn']
    query_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    forType = [{"dt": "231", "qt": "013901"}, {"dt": "232", "qt": "013903"}, {"dt": "233", "qt": "013904"},
               {"dt": "234", "qt": "013905"}, {"dt": "235", "qt": "013999"}, {"dt": "236", "qt": "0109"}]
    qtype = [{"qt": "ZQ_JLCF", "name": "纪律处分"}, {"qt": "ZQ_JGCS", "name": "监管措施"}, {"qt": "ZQ_WXHJ", "name": "问询函"}]

    def uniqueIDMaker(self):
        time_id = str(datetime.datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        end_time = str(self.query_time).split(" ")[0]
        # 其它
        link1 = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID="
        link2 = "&TABKEY=tab1&txtStartDate=2008-01-01&txtEndDate=" + str(end_time) + "&random=0.9791836623830588"
        for eachOne in self.qtype:
            sql_other = "SELECT MAX(publish_date),doc_source_url FROM securities_announcement_szse where belong_2_type=%s"
            self.cursor.execute(sql_other, eachOne["name"])
            latest_other = self.cursor.fetchone()
            other_latest_url = latest_other[1]
            type_name = eachOne["name"]
            pageNum = 1
            link = link1 + eachOne["qt"] + link2
            yield scrapy.Request(link, callback=self.forOtherParse,
                                 meta={"page": pageNum, "type_name": type_name, "end_time": end_time,
                                       "qt": eachOne["qt"], "latest_url": other_latest_url})
        # 债券公告
        url = "http://www.szse.cn/api/disc/announcement/annList?random=0.7254546809919651"
        for each in self.forType:
            flag = 0
            sql_bond = "SELECT MAX(disclosure_date),doc_source_url FROM securities_statement_index where " \
                       "user_create = 'zx'and detail_type=%s"
            self.cursor.execute(sql_bond, each["dt"])
            latest = self.cursor.fetchone()
            bond_latest_url = latest[1]
            page = 1
            detail_type = each["dt"]
            data = {
                "channelCode": ["companyDebt_disc"],
                "pageNum": "1",
                "pageSize": "30",
                "seDate": ["2007-01-01", end_time],
                "smallCategoryId": [each["qt"]]
            }
            yield scrapy.Request(
                url,
                method="POST",
                body=json.dumps(data),
                callback=self.parse,
                meta={"detail_type": detail_type, "page": page,
                      "end_time": end_time, "qt": each["qt"],
                      "latest_url": bond_latest_url, "flag": flag}
            )

    def parse(self, response):
        flag = response.meta["flag"]
        page = response.meta["page"]
        end_time = response.meta["end_time"]
        qt = response.meta["qt"]
        data_list = json.loads(response.body)["data"]
        latest_url = response.meta["latest_url"]
        for temp in data_list:
            item = ShenzhensecuritiesItem()
            item["detail_type"] = response.meta["detail_type"]
            item["doc_source_url"] = "http://disc.static.szse.cn/download/" + temp["attachPath"]
            item["disclosure_date"] = temp["publishTime"]
            if latest_url == item["doc_source_url"]:
                flag += 1
                break
            else:
                item["file_name"] = temp["title"]
                try:
                    item["bond_short_name"] = str(temp["title"]).split('：')[0]
                except:
                    item["bond_short_name"] = ""
                item["report_id"] = self.uniqueIDMaker()
                item["doc_local_path"] = "/volum3/homes3/ChinaSecurities/0000/" + item["report_id"] + ".pdf"
                item["country_code"] = "CHN"
                item["is_doc_url_direct"] = 1
                item["is_downloaded"] = 1
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["doc_downloaded_timestamp"] = item["gmt_create"]
                item["user_create"] = "zx"
                item["doc_type"] = "pdf"
                item["exchange_market_code"] = "SZSE"
                sql = "select id from securities_statement_index where doc_source_url=%s"
                self.cursor.execute(sql, item["doc_source_url"])
                result = self.cursor.fetchone()
                if result:
                    pass
                else:
                    yield item
        if len(data_list) == 30:
            if flag == 0:
                page += 1
                url = "http://www.szse.cn/api/disc/announcement/annList?random=0.7254546809919651"
                data = {
                    "channelCode": ["bondinfoNotice_disc"],
                    "pageNum": str(page),
                    "pageSize": "30",
                    "seDate": ["2008-01-01", end_time],
                    "smallCategoryId": [qt]
                }
                yield scrapy.Request(
                    url,
                    method="POST",
                    body=json.dumps(data),
                    callback=self.parse,
                    meta={"detail_type": response.meta["detail_type"],
                          "page": page, "end_time": end_time, "qt": qt,
                          "latest_url": latest_url, "flag": flag}
                )

    def forOtherParse(self, response):
        latest_url = response.meta["latest_url"]
        page = response.meta["page"]
        type_name = response.meta["type_name"]
        qt = response.meta["qt"]
        end_time = response.meta["end_time"]
        data_list = json.loads(response.body)[0]["data"]
        if len(data_list) > 0:
            for temp in data_list:
                item = ShenzhensecuritiesItemOther()
                item["object"] = temp["dx"]
                item["type"] = temp["lx"]
                item["number"] = temp["hh"]
                item["file_name"] = re.search(">(.*?)</a>", str(temp["hjbt"])).group(1)
                item["publish_date"] = temp["fhrq"]
                item["related_bond"] = temp["sjzh"]
                item["doc_source_url"] = "http://reportdocs.static.szse.cn" + re.search("encode-open='(.*?)'>", str(temp["hjbt"])).group(1)
                if latest_url == item["doc_source_url"]:
                    break
                else:
                    item["belong_2_type"] = type_name
                    item["report_id"] = self.uniqueIDMaker()
                    item["doc_local_path"] = "/volum3/homes3/ChinaSecurities/0000/" + item["report_id"] + ".pdf"
                    item["country_code"] = "CHN"
                    item["is_doc_url_direct"] = 1
                    item["is_downloaded"] = 1
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["doc_downloaded_timestamp"] = item["gmt_create"]
                    item["user_create"] = "zx"
                    item["doc_type"] = "pdf"
                    item["exchange_market_code"] = "SZSE"
                    sql = "select id from securities_announcement_szse where doc_source_url=%s"
                    self.cursor.execute(sql, item["doc_source_url"])
                    result = self.cursor.fetchone()
                    if result:
                        pass
                    else:
                        yield item
            page += 1
            link1 = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID="
            link2 = "&TABKEY=tab1&PAGENO=" + str(page) + "&txtStartDate=2008-01-01&txtEndDate=" + str(end_time) + \
                    "&random=0.9791836623830588"
            link = link1 + qt + link2
            yield scrapy.Request(link, callback=self.forOtherParse,
                                 meta={"page": page, "type_name": type_name, "end_time": end_time,
                                       "qt": qt, "latest_url": latest_url})
