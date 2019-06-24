# -*- coding: utf-8 -*-
import scrapy
import json
import time
from china.items import ChinaIntroItem_xsg


class CodeSpider(scrapy.Spider):
    name = 'shenzhen_xsgfjxyjc'
    allowed_domains = ['szse.cn']
    type_list_xsg = ["tab1", "tab2", "tab3"]
    url1_xsg = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1902&TABKEY="
    url2_xsg = "&PAGENO="
    url3_xsg = "&random=0.6829295020412363"

    def start_requests(self):
        for temp_xsg in self.type_list_xsg:
            page_xsg = 1
            url_xsg = self.url1_xsg + temp_xsg + self.url2_xsg + str(page_xsg) + self.url3_xsg
            yield scrapy.Request(
                    url_xsg,
                    callback=self.parse_xsg,
                    meta={"page": page_xsg, "belong_2_type": temp_xsg}
                    )

    def parse_xsg(self, response):
        belong_2_type = response.meta["belong_2_type"]
        page = response.meta["page"]
        if belong_2_type == "tab1":
            data_list = json.loads(response.body)[0]["data"]
            for temp in data_list:
                item = ChinaIntroItem_xsg()
                item["security_code"] = temp["zqdm"]
                item["exchange_market_code"] = "SZSE"
                item["release_sale_date"] = temp["jxrq"]
                item["release_sale_shareholder_num"] = temp["gdrs"]
                item["release_sale_stock_num"] = temp["jxsl"]
                item["release_sale_stock_rate"] = temp["jxbl"]
                item["shareholder_name"] = None
                item["managed_member_name"] = None
                item["disclosure_date"] = None
                item["bcpljcgf_num"] = None
                item["bcpljcgf_rate"] = None
                item["tgdzjyjc_num"] = None
                item["tgdzjyjc_rate"] = None
                item["jchrcygf_num"] = None
                item["jchrcygf_rate"] = None
                item["belong_2_type"] = "tab1"
                item["doc_source_url"] = None
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["spiderName"] = "shenzhen_xsgfjxyjc"
                yield item
        elif belong_2_type == "tab2":
            data_list = json.loads(response.body)[1]["data"]
            for temp in data_list:
                item = ChinaIntroItem_xsg()
                item["security_code"] = temp["zqdm"]
                item["exchange_market_code"] = "SZSE"
                item["release_sale_date"] = temp["jxrq"]
                item["release_sale_shareholder_num"] = None
                item["release_sale_stock_num"] = temp["jxsl"]
                item["release_sale_stock_rate"] = temp["jxbl"]
                item["shareholder_name"] = temp["gdmc"]
                item["managed_member_name"] = temp["hyjc"]
                item["disclosure_date"] = None
                item["bcpljcgf_num"] = None
                item["bcpljcgf_rate"] = None
                item["tgdzjyjc_num"] = None
                item["tgdzjyjc_rate"] = None
                item["jchrcygf_num"] = None
                item["jchrcygf_rate"] = None
                item["belong_2_type"] = "tab2"
                item["doc_source_url"] = None
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["spiderName"] = "shenzhen_xsgfjxyjc"
                yield item
        else:
            data_list = json.loads(response.body)[2]["data"]
            for temp in data_list:
                item = ChinaIntroItem_xsg()
                item["security_code"] = temp["zqdm"]
                item["exchange_market_code"] = "SZSE"
                item["release_sale_date"] = None
                item["release_sale_shareholder_num"] = None
                item["release_sale_stock_num"] = None
                item["release_sale_stock_rate"] = None
                item["shareholder_name"] = temp["gdmc"]
                item["managed_member_name"] = None
                item["disclosure_date"] = None
                item["bcpljcgf_num"] = temp["jcsl"]
                item["bcpljcgf_rate"] = temp["jcbl"]
                item["tgdzjyjc_num"] = temp["dzjcsl"]
                item["tgdzjyjc_rate"] = temp["dzjcbl"]
                item["jchrcygf_num"] = temp["cgsl"]
                item["jchrcygf_rate"] = temp["cgbl"]
                item["belong_2_type"] = "tab3"
                item["doc_source_url"] = None
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["spiderName"] = "shenzhen_xsgfjxyjc"
                yield item
        if len(data_list) != 0:
            page += 1
            url = self.url1_xsg + belong_2_type + self.url2_xsg + str(page) + self.url3_xsg
            yield scrapy.Request(
                url,
                callback=self.parse_xsg,
                meta={"page": page, "belong_2_type": belong_2_type}
            )
