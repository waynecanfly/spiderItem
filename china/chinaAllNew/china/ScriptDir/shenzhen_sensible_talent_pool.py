# -*- coding: utf-8 -*-
import scrapy
import json
import time
import re
from china.items import ChinaIntroItem_stp


class CodeSpider(scrapy.Spider):
    name = 'shenzhen_sensible_talent_pool'
    allowed_domains = ['szse.cn']
    url1_stp = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1900&TABKEY="
    url2_stp = "&PAGENO="
    url3_stp = "&random=0.23734333220651216"

    def start_requests(self):
            page_stp = 1
            url_stp = self.url1_stp + "tab3" + self.url2_stp + str(page_stp) + self.url3_stp
            yield scrapy.Request(
                    url_stp,
                    callback=self.parse_stp,
                    meta={"page": page_stp}
                    )

    def parse_stp(self, response):
        page = response.meta["page"]
        data_list = json.loads(response.body)[2]["data"]
        for temp in data_list:
            item = ChinaIntroItem_stp()
            xyxm = re.search(">(.*?)</a>", str(temp["xyxm"]))
            item["college_name"] = xyxm.group(1)
            item["exchange_market_code"] = "SZSE"
            item["training_date"] = temp["pxjsrq"]
            item["training_num"] = temp["pxqs"]
            item["gender"] = temp["xb"]
            item["age"] = temp["nl"]
            item["education"] = temp["xl"]
            item["discipline"] = temp["zy"]
            item["job_title"] = temp["zc"]
            item["doc_source_url"] = None
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "zx"
            item["spiderName"] = "shenzhen_sensible_talent_pool"
            yield item
        if len(data_list) != 0:
            page += 1
            url = self.url1_stp + "tab3" + self.url2_stp + str(page) + self.url3_stp
            yield scrapy.Request(
                url,
                callback=self.parse_stp,
                meta={"page": page}
            )
