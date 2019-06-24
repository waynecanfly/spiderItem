# -*- coding: utf-8 -*-
import scrapy
import json
import time
import re
import pymysql
from scrapy_splash import SplashRequest
from china.items import ChinaIntroItem


class CodeSpider(scrapy.Spider):
    # Initialization().InitializeMain2()
    name = 'HCK_information'
    allowed_domains = ['hkexnews.hk']
    # start_urls = ["http://www.hkexnews.hk/listedco/listconews/advancedsearch/search_active_main.aspx"]
    url1 = "http://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote?sym="
    url2 = "&sc_lang=en"
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,company_id from company_data_source where company_id like 'HKG%'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def start_requests(self):
        for temp in self.results:
            url = self.url1 + str(int(temp[0])) + self.url2
            yield scrapy.Request(url, callback=self.parse, meta={"security_code": temp[0], "company_id": temp[1]})

    def parse(self, response):
        item = ChinaIntroItem()
        item["code"] = response.meta["company_id"]
        try:
            #title
            item["company_profile_HCK"] = "company_profile"
            item["Issued_Shares_HCK"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[1]/div[1]/span[1]/text()').extract()[0]
            item["Industry_HCK"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[1]/div[2]/span[1]/text()').extract()[0]
            item["Listing_Date_HCK"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[1]/div[3]/span[1]/text()').extract()[0]
            item["Financial_Year_HCK"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[1]/div[4]/span[1]/text()').extract()[0]
            item["Chairman_HCK"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[2]/div[1]/span[1]/text()').extract()[0]
            item["Principal_Office_HCK"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[2]/div[2]/span[1]/text()').extract()[0]
            item["Place_of_Incorporation_HCK"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[2]/div[3]/span[1]/text()').extract()[0]
            item["Listing_Category_HCK"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[2]/div[4]/span[1]/text()').extract()[0]
            item["Registrar_HCK"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[2]/div[8]/span[1]/text()').extract()[0]
            #value
            item["company_profile"] = response.xpath('//div[@class="company_txt col_summary"]/text()').extract()[0]
            item["Issued_Shares"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[1]/div[1]/span[2]/text()').extract()[0]
            item["Industry"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[1]/div[2]/span[2]/span/span/text()').extract()[0]
            Listing_Date = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[1]/div[3]/span[2]/text()').extract()[0]
            try:
                date = str(Listing_Date).split(" ")[0]
                if int(date) < 10:
                    date = date.replace("1", "01").replace("2", "02").replace("3", "03").replace("4", "04").replace("5",
                            "05").replace("6", "06").replace("7", "07").replace("8", "08").replace("9", "09")
                item["Listing_Date"] = str(Listing_Date).split(" ")[-1] + "-" \
                                       + str(Listing_Date).split(" ")[1].replace("Jan", "01").replace("Feb", "02").replace("Mar",
                                        "03").replace("Apr", "04").replace("May", "05").replace("Jun", "06").replace("Jul",
                                        "07").replace("Aug", "08").replace("Sep", "09").replace("Oct", "10").replace("Nov",
                                        "11").replace("Dec", "12").replace("  ", "").replace(" ", "") \
                                       + "-" + date + " 00:00:00"
            except:
                item["Listing_Date"] = "-"
            item["Financial_Year"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[1]/div[4]/span[2]/text()').extract()[0]
            item["Chairman"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[2]/div[1]/span[2]/text()').extract()[0]
            item["Principal_Office"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[2]/div[2]/span[2]/text()').extract()
            item["Place_of_Incorporation"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[2]/div[3]/span[2]/text()').extract()[0]
            item["Listing_Category"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[2]/div[4]/span[2]/text()').extract()[0]
            item["Registrar"] = response.xpath('//div[@class="company_detail"]/div[@class="company_list"]/div[2]/div[8]/span[2]/a/text()').extract()[0]

            table_list = response.xpath('//table[@class="table_divi"]/tbody/tr')
            item_list = []
            item["entitlement_HCK"] = "entitlement"
            for temp in table_list:
                item_dict = {}
                item_dict["Date_Announced"] = temp.xpath('./td[1]/text()').extract()[0]
                item_dict["Ex_Date"] = temp.xpath('./td[2]/text()').extract()[0]
                item_dict["Details"] = temp.xpath('//table[@class="table_divi"]/tbody/tr[1]/td[3]/text()').extract()
                item_dict["Financial_Year_End"] = temp.xpath('./td[4]/text()').extract()[0]
                item_dict["Book_Close_Date"] = temp.xpath('./td[5]/text()').extract()[0]
                item_dict["Payment_Date"] = temp.xpath('./td[6]/text()').extract()[0]
                item_list.append(item_dict)
            item["entitlement"] = item_list
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "zx"
            item["doc_source_url"] = None
            yield item
        except:
            conn1 = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root",
                                    passwd="OPDATA",
                                    charset="utf8")
            cursor1 = conn1.cursor()
            sql = "insert into HCK_information_for_loss(company_id, not_have_url)value (%s,%s)"
            cursor1.execute(sql, [item["code"], response.url])
            conn1.commit()
            cursor1.close()
            conn1.close()
