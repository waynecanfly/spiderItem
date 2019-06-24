# -*- coding: utf-8 -*-
import scrapy
import json
import time
from china.items import ChinaIntroItem_shenzhen_status


class CodeSpider(scrapy.Spider):
    name = 'company_status'
    allowed_domains = ['szse.cn']
    table_list_status = ["1", "2"]
    url1_status = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1793_ssgs&TABKEY=tab"
    url2_status = "&PAGENO="
    url3_status = "&random=0.2831277777777923"

    def start_requests(self):
        for temp_status in self.table_list_status:
            num_status = 1
            if temp_status == "1":
                status = "pause"
            else:
                status = "delisting"
            link_status = self.url1_status + temp_status + self.url2_status
            url_status = link_status + str(num_status) + self.url3_status
            yield scrapy.Request(url_status, callback=self.parse_status, meta={"status": status, "link": link_status, "num": num_status})

    def parse_status(self, response):
        num = response.meta["num"]
        status = response.meta["status"]
        link = response.meta["link"]
        if status == "delisting":
            dataList = json.loads(response.body)[1]["data"]
            for temp_status in dataList:
                item = ChinaIntroItem_shenzhen_status()
                item["ipo_date"] = temp_status["ssrq"] + " 00:00:00"
                item["code"] = temp_status["zqdm"]
                item["end_date"] = temp_status["zzrq"] + " 00:00:00"
                item["gmt_update"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["doc_source_url"] = None
                item["status"] = status
                item["exchange_market_code"] = "SZSE"
                yield item
        else:
            dataList = json.loads(response.body)[0]["data"]
            for temp_status in dataList:
                item = ChinaIntroItem_shenzhen_status()
                item["ipo_date"] = temp_status["ssrq"] + " 00:00:00"
                item["code"] = temp_status["zqdm"]
                item["end_date"] = temp_status["ztrq"] + " 00:00:00"
                item["gmt_update"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["doc_source_url"] = None
                item["status"] = status
                item["exchange_market_code"] = "SZSE"
                yield item
        if len(dataList) != 0:
            num += 1
            url = link + str(num) + self.url3_status
            yield scrapy.Request(url, callback=self.parse, meta={"status": status, "link": link, "num": num})
