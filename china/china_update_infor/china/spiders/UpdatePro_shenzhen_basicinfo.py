# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
from scrapy_splash import SplashRequest
from china.items import ChinaIntroItem


class CodeSpider(scrapy.Spider):
    name = 'UpdatePro_shenzhen_basicinfo'
    allowed_domains = ['sse.com.cn']
    #start_urls = ["http://www.szse.cn/szseWeb/FrontController.szse?randnum=0.577506748569387"]
    start_urls = []
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,code from company where code like " + "'CHN%'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def start_requests(self):
        for temp in self.results:
            code = temp[0]
            company_id = temp[1]
            if int(code) < 600000:
                url = "http://www.szse.cn/szseWeb/FrontController.szse?randnum=0.577506748569387"
                data = {
                    "ACTIONID": "7",
                    "SOURCEURL": "/szseWeb/FrontController.szse*_QUESTION_*ACTIONID=7*_AND_*AJAX=AJAX-TRUE*_AND_*CATALOGID=1110*_AND_*TABKEY=tab2*_AND_*tab2PAGENO=1",
                    "SOURCECATALOGID": "1110",
                    "CATALOGID": "1743_detail_sme",
                    "TABKEY": "tab1",
                    "DM": str(code),
                    "site": "main"
                }
                yield scrapy.FormRequest(url, formdata=data, callback=self.parse, meta={"url": url, "code": company_id})

    def parse(self, response):
        url = response.meta["url"]
        code = response.meta["code"]
        item = ChinaIntroItem()
        #网页爬取数据--company
        item["name_origin"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[1]/td[2]/text()').extract()[0]
        item["name_en"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[2]/td[2]/text()').extract()[0]
        item["security_code"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[4]/td[2]/text()').extract()[0]
        ipo_date = response.xpath('//table[@id="1743_detail_smetab1"]//tr[5]/td[2]/text()').extract()[0]
        item["ipo_date"] = str(ipo_date) + " 00:00:00"
        website_url = response.xpath('//table[@id="1743_detail_smetab1"]//tr[10]/td[2]/a/text()').extract()
        if len(website_url) == 0:
            item["website_url"] = None
        else:
            item["website_url"] = website_url[0]
        #自定义添加的数据--company
        item["code"] = code
        item["country_code_listed"] = "CHN"
        item["country_code_origin"] = "CHN"
        item["exchange_market_code"] = "SZSE"
        item["currency_code"] = "CNY"
        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item["user_create"] = "root"

        #网页爬取数据--detail title
        item["registered_address_title"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[3]/td[1]/text()').extract()[0]
        item["company_short_name_zh_title"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[4]/td[3]/text()').extract()[0]
        item["Total_share_capital_of_A_shares_title"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[5]/td[3]/text()').extract()[0]
        item["A_shares_circulating_capital_title"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[5]/td[5]/text()').extract()[0]
        item["district_belong_to_title"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[8]/td[5]/text()').extract()[0]
        item["industry_title"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[9]/td[1]/text()').extract()[0]
        #网页爬取数据--detail value
        item["registered_address"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[3]/td[2]/text()').extract()[0]
        item["company_short_name_zh"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[4]/td[4]/text()').extract()[0]
        item["Total_share_capital_of_A_shares"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[5]/td[4]/text()').extract()[0]
        item["A_shares_circulating_capital"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[5]/td[6]/text()').extract()[0]
        item["district_belong_to"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[8]/td[6]/text()').extract()[0]
        item["industry"] = response.xpath('//table[@id="1743_detail_smetab1"]//tr[9]/td[2]/text()').extract()[0]
        yield item
        """
        for temp in item:
            print(temp)
            print(str(item[temp]).encode("utf-8"))
        """