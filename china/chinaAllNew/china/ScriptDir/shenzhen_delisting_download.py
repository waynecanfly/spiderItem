# -*- coding: utf-8 -*-
import scrapy
import json
import time
import pymysql
from china.items import ChinaIntroItem_dd


class CodeSpider(scrapy.Spider):
    query_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    name = 'shenzhen_delisting_download'
    allowed_domains = ['szse.cn']
    page_dd = 1
    newstdate_dict_dd = {}
    code_list_dd = []
    report_num_dict_dd = {}
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()

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
        end_time = str(self.query_time).split(" ")[0]
        url_dd = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
        data_dd = {
            "seDate": ["2008-01-01", end_time],
            "channelCode": ["delist_disc"],
            "pageSize": "30",
            "pageNum": str(self.page_dd)
        }
        yield scrapy.Request(
                url_dd,
                method="POST",
                body=json.dumps(data_dd),
                callback=self.parse_dd,
                meta={"end_time": end_time}
                )

    def parse_dd(self, response):
        end_time = response.meta["end_time"]
        data_list = json.loads(response.body)["data"]
        if len(data_list) != 0:
            for temp in data_list:
                item = ChinaIntroItem_dd()
                item["disclosure_date"] = temp["publishTime"]
                title = temp["title"]
                item["doc_source_url"] = "http://disc.static.szse.cn/download" + temp["attachPath"]
                item["exchange_market_code"] = "SZSE"
                item["security_code"] = temp["secCode"][0]
                try:
                    if item["security_code"] not in self.code_list_dd:
                        sql_select = "select max(disclosure_date) from shenzhen_delisting_announcement where security_code = %s group by security_code"
                        self.cursor.execute(sql_select, item["security_code"])
                        result = self.cursor.fetchone()
                        self.newstdate_dict_dd[item["security_code"]] = int(str(result[0]).replace("-", "").replace(" ", "").replace(":", ""))
                        self.code_list_dd.append(item["security_code"])
                        self.report_num_dict_dd[item["security_code"]] = "000"
                    # print("%s,%s,%s" % (item["security_code"], self.newstdate_dict_dd[item["security_code"]], item["disclosure_date"]))
                    if int(str(item["disclosure_date"]).replace("-", "").replace(" ", "").replace(":", "")) > self.newstdate_dict_dd[item["security_code"]]:
                        num = int(self.report_num_dict_dd[item["security_code"]]) + 1
                        num = self.go_heavy_num(num)
                        item["report_id"] = item["security_code"] + "0000" + "00" + "07" + "01" + num
                        self.report_num_dict_dd[item["security_code"]] = num
                        item["doc_local_path"] = "/volume1/homes/ChinaNon/" + "0000" + "/" + item["report_id"] + ".pdf"
                        item["is_doc_url_direct"] = 1
                        item["doc_type"] = "pdf"
                        item["is_downloaded"] = 1
                        item["language_written_code"] = "zh-simple"
                        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        item["user_create"] = "zx"
                        item["file_name"] = title
                        item["spiderName"] = "shenzhen_delisting_download"
                        yield item
                except TypeError:
                    if item["security_code"] not in self.code_list_dd:
                        self.code_list_dd.append(item["security_code"])
                        self.report_num_dict_dd[item["security_code"]] = "000"
                    num = int(self.report_num_dict_dd[item["security_code"]]) + 1
                    num = self.go_heavy_num(num)
                    item["report_id"] = item["security_code"] + "0000" + "00" + "07" + "01" + num
                    self.report_num_dict_dd[item["security_code"]] = num
                    item["doc_local_path"] = "/volume1/homes/ChinaNon/" + "0000" + "/" + item["report_id"] + ".pdf"
                    item["is_doc_url_direct"] = 1
                    item["doc_type"] = "pdf"
                    item["is_downloaded"] = 1
                    item["language_written_code"] = "zh-simple"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["user_create"] = "zx"
                    item["file_name"] = title
                    item["spiderName"] = "shenzhen_delisting_download"
                    yield item
            self.page_dd += 1
            url = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
            data = {
                "seDate": ["2008-01-01", end_time],
                "channelCode": ["delist_disc"],
                "pageSize": "30",
                "pageNum": str(self.page_dd)
            }
            yield scrapy.Request(
                url,
                method="POST",
                body=json.dumps(data),
                callback=self.parse_dd,
                meta={"end_time": end_time}
            )
