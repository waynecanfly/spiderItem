# -*- coding: utf-8 -*-
import scrapy
import json
import time
import re
import pymysql
from china.items import ChinaIntroItem_if


class CodeSpider(scrapy.Spider):
    name = 'shenzhen_integrity_file'
    allowed_domains = ['szse.cn']
    type_list_if = ["1759_cxda", "1903_detail"]
    url1_if = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID="
    url2_if = "&TABKEY=tab1&PAGENO="
    url3_if = "&random=0.17988388377929776"
    code_list_if = []
    report_num_dict_if = {}
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
        for temp_if in self.type_list_if:
            if temp_if == "1759_cxda":
                belong2type_if = "处罚与处分记录"
            else:
                belong2type_if = "中介机构处罚与处分记录"
            sql_if = "select MAX(punish_date) FROM shenzhen_integrity_file where belong_2_type =%s"
            self.cursor.execute(sql_if, belong2type_if)
            newstdate_if = int(str(self.cursor.fetchone()[0]).replace("-", ""))
            page_if = 1
            url_if = self.url1_if + temp_if + self.url2_if + str(page_if) + self.url3_if
            yield scrapy.Request(
                    url_if,
                    callback=self.parse_if,
                    meta={"belong_2_type": temp_if, "page": page_if, "newstdate": newstdate_if}
                    )

    def parse_if(self, response):
        newstdate = response.meta["newstdate"]
        page = response.meta["page"]
        belong_2_type = response.meta["belong_2_type"]
        data_list = json.loads(response.body)[0]["data"]
        if len(data_list) != 0:
            if belong_2_type == "1759_cxda":
                for temp in data_list:
                    item = ChinaIntroItem_if()
                    title = temp["bt"]
                    link = temp["ck"]
                    data = re.search("encode-open='(.*?)'>", link)
                    item["punish_date"] = temp["xx_fwrq"]
                    if int(str(item["punish_date"]).replace("-", "")) > newstdate:
                        item["litigant"] = temp["dsr"]
                        item["punish_type"] = temp["cflb"]
                        item["doc_source_url"] = "http://www.szse.cn" + data.group(1)
                        item["exchange_market_code"] = "SZSE"
                        item["security_code"] = temp["xx_gsdm"]
                        if item["security_code"] not in self.code_list_if:
                            self.code_list_if.append(item["security_code"])
                            self.report_num_dict_if[item["security_code"]] = "000"
                        num = int(self.report_num_dict_if[item["security_code"]]) + 1
                        num = self.go_heavy_num(num)
                        item["report_id"] = item["security_code"] + "0000" + "00" + "07" + "01" + num
                        self.report_num_dict_if[item["security_code"]] = num
                        item["doc_local_path"] = "/volume1/homes/ChinaNon/" + "0000" + "/" + item["report_id"] + ".pdf"
                        item["is_doc_url_direct"] = 1
                        item["doc_type"] = "pdf"
                        item["is_downloaded"] = 1
                        item["language_written_code"] = "zh-simple"
                        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        item["user_create"] = "zx"
                        item["file_name"] = title
                        item["agency_name"] = None
                        item["agency_type"] = None
                        item["belong_2_type"] = "处罚与处分记录"
                        item["spiderName"] = "shenzhen_integrity_file"
                        yield item
            else:
                for temp in data_list:
                    item = ChinaIntroItem_if()
                    title = temp["zj_bt"]
                    link = temp["ck"]
                    data = re.search("encode-open='(.*?)'>", link)
                    item["punish_date"] = temp["zj_fwrq"]
                    if int(str(item["punish_date"]).replace("-", "")) > newstdate:
                        item["litigant"] = temp["zj_dsr"]
                        item["punish_type"] = temp["zj_cflb"]
                        item["doc_source_url"] = "http://www.szse.cn" + data.group(1)
                        item["exchange_market_code"] = "SZSE"
                        item["security_code"] = temp["zj_gsdm"]
                        if item["security_code"] not in self.code_list_if:
                            self.code_list_if.append(item["security_code"])
                            self.report_num_dict_if[item["security_code"]] = "000"
                        num = int(self.report_num_dict_if[item["security_code"]]) + 1
                        num = self.go_heavy_num(num)
                        item["report_id"] = item["security_code"] + "0000" + "00" + "07" + "01" + num
                        self.report_num_dict_if[item["security_code"]] = num
                        item["doc_local_path"] = "/volume1/homes/ChinaNon/" + "0000" + "/" + item["report_id"] + ".pdf"
                        item["is_doc_url_direct"] = 1
                        item["doc_type"] = "pdf"
                        item["is_downloaded"] = 1
                        item["language_written_code"] = "zh-simple"
                        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        item["user_create"] = "zx"
                        item["file_name"] = title
                        item["agency_name"] = temp["zj_zjrm"]
                        item["agency_type"] = temp["zj_cfdx"]
                        item["belong_2_type"] = "中介机构处罚与处分记录"
                        item["spiderName"] = "shenzhen_integrity_file"
                        yield item
            page += 1
            url = self.url1_if + belong_2_type + self.url2_if + str(page) + self.url3_if
            yield scrapy.Request(
                url,
                callback=self.parse_if,
                meta={"belong_2_type": belong_2_type, "page": page, "newstdate": newstdate}
            )
