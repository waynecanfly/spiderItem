# -*- coding: utf-8 -*-
import scrapy
import time
import re
import json
from china.items import ChinaIntroItem_shenzhenL
from china.items import ChinaIntroItem_shanghaiL


class CodeSpider(scrapy.Spider):
    name = 'china_updateAll_l'
    allowed_domains = ['sse.com.cn', 'szse.cn']
    #shanghaiL 无
    #shenzhenL
    type_list_shenzhenL = ["tab2", "tab3", "tab4"]
    url1_shenzhenL = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1110x&TABKEY="
    url2_shenzhenL = "&PAGENO="
    url3_shenzhenL = "&random=0.8452698015484685"

    def start_requests(self):
        #shanghaiL
        url_shanghaiL = "http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1"
        yield scrapy.Request(url_shanghaiL, callback=self.parse_shanghaiL)
        #shenzhenL
        for temp_shenzhenL in self.type_list_shenzhenL:
            num_shenzhenL = 1
            if temp_shenzhenL == "tab2":
                plate_type = "主版"#1
            elif temp_shenzhenL == "tab3":
                plate_type = "中小企业版"#2
            else:
                plate_type = "创业版"#3
            link_shenzhenL = self.url1_shenzhenL + temp_shenzhenL + self.url2_shenzhenL
            url_shenzhenL = self.url1_shenzhenL + temp_shenzhenL + self.url2_shenzhenL + str(num_shenzhenL) + self.url3_shenzhenL
            yield scrapy.FormRequest(url_shenzhenL, callback=self.parse_shenzhenL, meta={"link": link_shenzhenL, "plate_type": plate_type, "num": num_shenzhenL})

    def parse_shanghaiL(self, response):
        data = response.text
        item = ChinaIntroItem_shanghaiL()
        item["data"] = data
        item["doc_source_url"] = None
        yield item
        # with open("/data/OPDCMS/china/listed_company_update/company_list/shanghai_" + self.create_time_shanghaiL + ".txt", "a") as f:
        #     f.write(data)

    def parse_shenzhenL(self, response):
        num = response.meta["num"]
        plate_type = response.meta["plate_type"]
        link = response.meta["link"]
        data = response.body
        if plate_type == "主版":
            data_list = json.loads(data)[1]["data"]
        elif plate_type == "中小企业版":
            data_list = json.loads(data)[2]["data"]
        else:
            data_list = json.loads(data)[3]["data"]
        if len(data_list) != 0:
            for temp_shenzhenL in data_list:
                item = ChinaIntroItem_shenzhenL()
                item["plate_type"] = plate_type
                item["code"] = temp_shenzhenL["zqdm"]
                item["company_short_name_zh"] = re.search("><u>(.*?)</u></a>", str(temp_shenzhenL["gsjc"])).group(1)
                item["name_origin"] = temp_shenzhenL["gsqc"]
                item["security_code"] = temp_shenzhenL["sshymc"]
                item["website_url"] = temp_shenzhenL["http"]
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["doc_source_url"] = None
                yield item
                num += 1
                url = link + str(num) + self.url3_shenzhenL
                yield scrapy.Request(url, callback=self.parse_shenzhenL, meta={"link": link, "plate_type": plate_type, "num": num})
