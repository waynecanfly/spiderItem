# -*- coding: utf-8 -*-
import scrapy
import json
import time
import pymysql
from china.items import ChinaIntroItem_cs
from china.Initialization import Initialization


class CodeSpider(scrapy.Spider):
    query_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    name = 'shenzhen_continuous_supervision'
    allowed_domains = ['szse.cn']
    type_code_cs = ["sponsor_disc", "finance_disc"]
    code_list_cs = []
    jud_list_cs = []
    newstdate_dict_cs = {}
    report_num_dict_cs = {}
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
        Initialization().InitializeMain3()
        end_time = str(self.query_time).split(" ")[0]
        for temp_cs in self.type_code_cs:
            page_cs = 1
            url_cs = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
            data_cs = {
                "seDate": ["2008-01-01", end_time],
                "channelCode": [temp_cs],
                "pageSize": "30",
                "pageNum": str(page_cs)
            }
            yield scrapy.Request(
                    url_cs,
                    method="POST",
                    body=json.dumps(data_cs),
                    callback=self.parse_cs,
                    meta={"belong_2_type": temp_cs, "page": page_cs, "end_time": end_time}
                    )

    def parse_cs(self, response):
        end_time = response.meta["end_time"]
        page = response.meta["page"]
        belong_2_type = response.meta["belong_2_type"]
        data_list = json.loads(response.body)["data"]
        if len(data_list) != 0:
            for temp in data_list:
                item = ChinaIntroItem_cs()
                item["disclosure_date"] = temp["publishTime"]
                title = temp["title"]
                item["doc_source_url"] = "http://disc.static.szse.cn/download" + temp["attachPath"]
                item["exchange_market_code"] = "SZSE"
                item["security_code"] = temp["secCode"][0]
                jud = item["security_code"] + "@@@" + belong_2_type
                if belong_2_type == "finance_disc":
                    item["belong_2_type"] = "财务顾问持续督导意见"
                else:
                    item["belong_2_type"] = "保荐机构持续督导意见"
                if item["security_code"] not in self.code_list_cs:
                    self.code_list_cs.append(item["security_code"])
                    self.report_num_dict_cs[item["security_code"]] = "000"
                try:
                    if jud not in self.jud_list_cs:
                        self.jud_list_cs.append(jud)
                        sql_select = "select max(disclosure_date) from shenzhen_continuous_supervision where security_code = %s" \
                                     " and belong_2_type = %s group by security_code,belong_2_type"
                        self.cursor.execute(sql_select, [item["security_code"], item["belong_2_type"]])
                        result = self.cursor.fetchone()[0]
                        self.newstdate_dict_cs[item["security_code"]] = int(str(result).replace("-", "").replace(" ", "").replace(":", ""))
                    if int(str(item["disclosure_date"]).replace("-", "").replace(" ", "").replace(":", "")) > self.newstdate_dict_cs[item["security_code"]]:
                        # print("%s,%s,%s" % (item["security_code"], self.newstdate_dict_cs[item["security_code"]], item["disclosure_date"]))
                        num = int(self.report_num_dict_cs[item["security_code"]]) + 1
                        num = self.go_heavy_num(num)
                        item["report_id"] = item["security_code"] + "0000" + "00" + "07" + "01" + num
                        self.report_num_dict_cs[item["security_code"]] = num
                        item["doc_local_path"] = "/volume1/homes/ChinaNon/" + "0000" + "/" + item["report_id"] + ".pdf"
                        item["is_doc_url_direct"] = 1
                        item["doc_type"] = "pdf"
                        item["is_downloaded"] = 1
                        item["language_written_code"] = "zh-simple"
                        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        item["user_create"] = "zx"
                        item["file_name"] = title
                        item["spiderName"] = "shenzhen_continuous_supervision"
                        yield item
                except TypeError:
                    num = int(self.report_num_dict_cs[item["security_code"]]) + 1
                    num = self.go_heavy_num(num)
                    item["report_id"] = item["security_code"] + "0000" + "00" + "07" + "01" + num
                    self.report_num_dict_cs[item["security_code"]] = num
                    item["doc_local_path"] = "/volume1/homes/ChinaNon/" + "0000" + "/" + item["report_id"] + ".pdf"
                    item["is_doc_url_direct"] = 1
                    item["doc_type"] = "pdf"
                    item["is_downloaded"] = 1
                    item["language_written_code"] = "zh-simple"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["user_create"] = "zx"
                    item["file_name"] = title
                    item["spiderName"] = "shenzhen_continuous_supervision"
                    yield item
            page += 1
            url = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
            data = {
                "seDate": ["2008-01-01", end_time],
                "channelCode": [belong_2_type],
                "pageSize": "30",
                "pageNum": str(page)
            }
            yield scrapy.Request(
                url,
                method="POST",
                body=json.dumps(data),
                callback=self.parse_cs,
                meta={"belong_2_type": belong_2_type, "page": page, "end_time": end_time}
            )
