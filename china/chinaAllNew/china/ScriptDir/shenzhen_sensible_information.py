# -*- coding: utf-8 -*-
import scrapy
import json
import time
from china.items import ChinaIntroItem_sbi


class CodeSpider(scrapy.Spider):
    name = 'shenzhen_sensible_information'
    allowed_domains = ['szse.cn']
    url1_sbi = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1900&TABKEY="
    url2_sbi = "&PAGENO="
    url3_sbi = "&random=0.23734333220651216"
    table_key_list_sbi = ["tab1", "tab2"]

    def start_requests(self):
        for temp_sbi in self.table_key_list_sbi:
            page_sbi = 1
            url_sbi = self.url1_sbi + temp_sbi + self.url2_sbi + str(page_sbi) + self.url3_sbi
            sensible_type = temp_sbi
            yield scrapy.Request(
                    url_sbi,
                    callback=self.parse_sbi,
                    meta={"sensible_type": sensible_type, "page": page_sbi}
                    )

    def parse_sbi(self, response):
        page = response.meta["page"]
        sensible_type = response.meta["sensible_type"]
        if sensible_type == "tab1":
            data_list = json.loads(response.body)[0]["data"]
            for temp in data_list:
                item = ChinaIntroItem_sbi()
                item["file_name"] = temp["dldsxm"]
                item["exchange_market_code"] = "SZSE"
                item["security_code"] = temp["gsdm"]
                item["disclosure_date"] = temp["gsqj"]
                item["doc_type"] = "拟聘"
                item["doc_source_url"] = None
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["spiderName"] = "shenzhen_sensible_information"
                yield item
        else:
            data_list = json.loads(response.body)[1]["data"]
            for temp in data_list:
                item = ChinaIntroItem_sbi()
                item["file_name"] = temp["dldsxm"]
                item["exchange_market_code"] = "SZSE"
                item["security_code"] = temp["gsdm"]
                item["doc_type"] = "在任"
                item["disclosure_date"] = None
                item["doc_source_url"] = None
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["spiderName"] = "shenzhen_sensible_information"
                yield item
        if len(data_list) != 0:
            page += 1
            url = self.url1_sbi + sensible_type + self.url2_sbi + str(page) + self.url3_sbi
            yield scrapy.Request(
                url,
                callback=self.parse_sbi,
                meta={"sensible_type": sensible_type, "page": page}
            )
