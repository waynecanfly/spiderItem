# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
from china.items import ChinaIntroItem_shanghai
from china.items import ChinaIntroItem_shenzhen


class ShangHaiSpider(scrapy.Spider):
    name = 'china_updateAll_c'
    allowed_domains = ['sse.com.cn', 'sse.com.cn']
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    #shanghai_basicinfo
    flag_shanghai = 0
    sql_shanghai = "select security_code,company_id from company_data_source where company_id like " + "'CHN%' and mark = '0'"
    # sql_shanghai = "select security_code,company_id from company_data_source where company_id like 'CHN%' and " \
    #                 "user_create = 'zx' and infor_is_downloaded='0'"
    cursor.execute(sql_shanghai)
    results_shanghai = cursor.fetchall()
    #shenzhen_basicinfo
    url1_shenzhen = "http://www.szse.cn/certificate/individual/index.html?code="
    # sql_shenzhen = "select security_code,company_id from company_data_source where company_id like " + "'CHN%' and mark = '0'"
    # cursor.execute(sql_shenzhen)
    results_shenzhen = results_shanghai

    def start_requests(self):
        #shanghaibasic_info
        for temp_shanghai in self.results_shanghai:
            code_shanghai = temp_shanghai[0]
            company_id_shanghai = temp_shanghai[1]
            if int(code_shanghai) >= 600000:
                url_shanghai = "http://www.sse.com.cn/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=" + str(code_shanghai)
                yield scrapy.Request(url_shanghai, callback=self.parse_shanghai, meta={"company_id": company_id_shanghai})
        #shenzhen_basicinfo
        for temp_shenzhen in self.results_shenzhen:
            code_shenzhen = temp_shenzhen[0]
            company_id_shenzhen = temp_shenzhen[1]
            if int(code_shenzhen) < 600000:
                url_shenzhen = self.url1_shenzhen + code_shenzhen
                yield scrapy.Request(url_shenzhen, callback=self.parse_shenzhen, meta={"url": url_shenzhen, "code": company_id_shenzhen})

    def parse_shanghai(self, response):
        company_id = response.meta["company_id"]
        item = ChinaIntroItem_shanghai()
        # 网页爬取的数据--company
        try:
            item["security_code"] = response.xpath('//table[@class="table search_"]/tbody/tr[1]/td/text()').extract()[0]
            ipo_date = str(response.xpath('//table[@class="table search_"]//tr[3]/td/a[@target="_blank"]/text()').extract()[0]).split("/")[0]
            item["ipo_date"] = str(ipo_date) + " 00:00:00"
            item["name_origin"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[6]/td/text()').extract()[0]).split("/")[0]
            item["name_en"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[6]/td/text()').extract()[0]).split("/")[-1]
            website_url = response.xpath('//table[@class="table search_"]/tbody/tr[13]/td/a/text()').extract()
            if len(website_url) == 0:
                item["website_url"] = None
            else:
                item["website_url"] = website_url[0]
            item["status"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[17]/td/text()').extract()[0]).split("/")[0]
            # 自定义添加的数据--company
            item["code"] = company_id
            item["country_code_listed"] = "CHN"
            item["country_code_origin"] = "CHN"
            item["exchange_market_code"] = "SSE"
            item["currency_code"] = "CNY"
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "root"

            # 网页爬取的数据--detail-value
            item["convertible_bonds_for_short"] = response.xpath('//table[@class="table search_"]/tbody/tr[4]/td/text()').extract()[0]
            item["company_short_name_zh"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[5]/td/text()').extract()[0]).split("/")[0]
            item["company_short_name_en"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[5]/td/text()').extract()[0]).split("/")[-1]
            item["registered_address"] = response.xpath('//table[@class="table search_"]/tbody/tr[7]/td/text()').extract()[0]
            item["mailing_address"] = response.xpath('//table[@class="table search_"]/tbody/tr[8]/td/text()').extract()[0]
            item["legal_representative"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[9]/td/text()').extract()[0]).replace(" ", "")
            item["secretary_name"] = response.xpath('//table[@class="table search_"]/tbody/tr[10]/td/text()').extract()[0]
            item["e_mail"] = response.xpath('//table[@class="table search_"]/tbody/tr[11]/td/a/text()').extract()[0]
            item["phone_number"] = response.xpath('//table[@class="table search_"]/tbody/tr[12]/td/text()').extract()[0]
            item["CSRC_industry"] = response.xpath('//table[@class="table search_"]/tbody/tr[14]/td/text()').extract()[0]
            item["SSE_industry"] = response.xpath('//table[@class="table search_"]/tbody/tr[15]/td/text()').extract()[0]
            item["district_belong_to"] = response.xpath('//table[@class="table search_"]/tbody/tr[16]/td/text()').extract()[0]
            item["is_SSE_180_sample_stock"] = response.xpath('//table[@class="table search_"]/tbody/tr[18]/td/text()').extract()[0]
            item["is_overseas_listing"] = response.xpath('//table[@class="table search_"]/tbody/tr[19]/td/text()').extract()[0]
            item["overseas_listing_land"] = response.xpath('//table[@class="table search_"]/tbody/tr[20]/td/text()').extract()[0]
            # 网页爬取的数据--detail-title
            item["convertible_bonds_for_short_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[4]/th/text()').extract()[0]
            item["company_short_name_zh_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[5]/th/text()').extract()[0]
            item["company_short_name_en_title"] = item["company_short_name_zh_title"]
            item["registered_address_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[7]/th/text()').extract()[0]
            item["mailing_address_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[8]/th/text()').extract()[0]
            item["legal_representative_title"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[9]/th/text()').extract()[0]).replace(" ", "")
            item["secretary_name_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[10]/th/text()').extract()[0]
            item["e_mail_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[11]/th/text()').extract()[0]
            item["phone_number_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[12]/th/text()').extract()[0]
            item["CSRC_industry_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[14]/th/text()').extract()[0]
            item["SSE_industry_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[15]/th/text()').extract()[0]
            item["district_belong_to_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[16]/th/text()').extract()[0]
            item["is_SSE_180_sample_stock_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[18]/th/text()').extract()[0]
            item["is_overseas_listing_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[19]/th/text()').extract()[0]
            item["overseas_listing_land_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[20]/th/text()').extract()[0]
            item["doc_source_url"] = None
            yield item
        except:
            print("还没有基本信息(SSE)")

    def parse_shenzhen(self, response):
        url = response.meta["url"]
        code = response.meta["code"]
        item = ChinaIntroItem_shenzhen()
        try:
            #网页爬取数据--company
            item["name_origin"] = response.xpath('//div[@class="g-contitle g-contitle-reset mt30"]/h2/text()').extract()[0]
            item["name_en"] = response.xpath('//div/h4/text()').extract()[0]
            item["security_code"] = response.xpath('//table[@class="table table-border"]//tr[2]/td[2]/text()').extract()[0]
            ipo_date = response.xpath('//table[@class="table table-border"]//tr[4]/td[2]/text()').extract()[0]
            item["ipo_date"] = str(ipo_date) + " 00:00:00"
            website_url = response.xpath('//table[@class="table table-border"]//tr[9]/td[2]/a/text()').extract()
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
            item["registered_address_title"] = response.xpath('//table[@class="table table-border"]//tr[1]/td[1]/text()').extract()[0]
            item["company_short_name_zh_title"] = response.xpath('//table[@class="table table-border"]//tr[3]/td[1]/text()').extract()[0]
            item["Total_share_capital_of_A_shares_title"] = response.xpath('//table[@class="table table-border"]//tr[5]/td[1]/text()').extract()[0]
            item["A_shares_circulating_capital_title"] = response.xpath('//table[@class="table table-border"]//tr[6]/td[1]/text()').extract()[0]
            item["district_belong_to_title"] = response.xpath('//table[@class="table table-border"]//tr[8]/td[1]/text()').extract()[0]
            item["industry_title"] = "所属行业"
            #网页爬取数据--detail value
            item["registered_address"] = response.xpath('//table[@class="table table-border"]//tr[1]/td[2]/text()').extract()[0]
            item["company_short_name_zh"] = response.xpath('//table[@class="table table-border"]//tr[3]/td[2]/text()').extract()[0]
            item["Total_share_capital_of_A_shares"] = response.xpath('//table[@class="table table-border"]//tr[5]/td[2]/text()').extract()[0]
            item["A_shares_circulating_capital"] = response.xpath('//table[@class="table table-border"]//tr[6]/td[2]/text()').extract()[0]
            item["district_belong_to"] = response.xpath('//table[@class="table table-border"]//tr[8]/td[2]/text()').extract()[0]
            item["industry"] = response.xpath('//table[@class="table table-border"]//tr[8]/td[4]/text()').extract()[0]
            item["doc_source_url"] = None
            yield item
        except:
            print("A股无数据")
