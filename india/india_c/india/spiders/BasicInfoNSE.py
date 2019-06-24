# -*- coding: utf-8 -*-
import scrapy
import csv
import pymysql
import os
import time
from india.items import IndiaItem


class IndiaSpider(scrapy.Spider):
    name = 'BasicInfoNSE'
    allowed_domains = ['nseindia.com']
    url1 = "https://www.nseindia.com/marketinfo/companyTracker/compInfo.jsp?symbol="
    url2 = "&series=EQ"
    link = "https://www.nseindia.com/marketinfo/companyTracker/corpAction.jsp?symbol="
    code_list = []
    company_id_list = []
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select info_disclosure_id,company_id from company_data_source where company_id like " + "'IND%' and spider_name = 'BasicInfoNSE'"
    cursor.execute(sql)
    results = cursor.fetchall()
    for temp in results:
        code_list.append(temp[0])
        company_id_list.append(temp[1])

    def get_newest_company_file(self, name):
        """获取最新的公司文件"""
        name_list = []
        name_num_list = []
        dir_list = os.listdir("/data/OPDCMS/india/listed_company_update/company_list")
        for temp in dir_list:
            if name in temp:
                name_title = str(temp).split(".")[0]
                name_list.append(name_title)
        for each in name_list:
            name_num = str(each).split("_")[-1]
            name_num_list.append(int(name_num))
        name_num_list.sort()
        newest_name_num = str(name_num_list[-1])
        return newest_name_num

    def start_requests(self):
        newest_name_num = self.get_newest_company_file("NSE")
        excel_name = "/data/OPDCMS/india/listed_company_update/company_list/NSE_" + newest_name_num + ".csv"
        f = open(excel_name, "r")
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= 1:
                Symbol = row[0]
                if Symbol in self.code_list:
                    item = IndiaItem()
                    item["security_code"] = None
                    item["info_disclosure_id"] = Symbol
                    iNum = self.code_list.index(Symbol)
                    item["company_id"] = self.company_id_list[iNum]
                    item["name_origin"] = row[1]
                    item["name_en"] = item["name_origin"]
                    item["First_Listing_Date"] = row[2]
                    item["Face_Value"] = row[3]
                    item["Paid_Up_Value"] = row[4]
                    item["Market_Lot"] = row[5]
                    item["ISIN"] = row[6]
                    item["country_code_listed"] = "IND"
                    item["exchange_market_code"] = "BSE"
                    item["currency_code"] = "INR"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["user_create"] = "root"
                    item["website_url"] = None
                    item["status"] = None
                    url = self.url1 + str(Symbol) + self.url2
                    link = self.link + str(Symbol)
                    yield scrapy.Request(url, callback=self.parse, meta={"item": item, "link": link})

    def parse(self, response):
        item = response.meta["item"]
        link = response.meta["link"]
        Date_of_Listing = response.xpath('//table//tr[2]/td/text()').extract()
        if len(Date_of_Listing) == 0:
            item["ipo_date"] = None
        else:
            data = Date_of_Listing[0]
            data = data.split("-")
            mon = str(data[1]).replace("Jan", "01").replace("Feb", "02").replace("Mar", "03").replace("Apr", "04").replace("May", "05").replace("Jun", "06").replace("Jul", "07").replace("Aug", "08").replace("Sep", "09").replace("Oct", "10").replace("Nov", "11").replace("Dec", "12")
            item["ipo_date"] = data[-1] + "-" + mon + "-" + data[0] + " 00:00:00"
        Industry = response.xpath('//table//tr[5]/td/text()').extract()
        if len(Industry) == 0:
            item["Industry"] = None
        else:
            item["Industry"] = Industry[0]
        Issued_Cap = response.xpath('//table//tr[7]/td/text()').extract()
        if len(Issued_Cap) == 0:
            item["Issued_Cap"] = None
        else:
            item["Issued_Cap"] = Issued_Cap[0].replace("  ", "")
        Free_Float_Market_Cap = response.xpath('//table//tr[8]/td/text()').extract()
        if len(Free_Float_Market_Cap) == 0:
            item["Free_Float_Market_Cap"] = None
        else:
            item["Free_Float_Market_Cap"] = Free_Float_Market_Cap[0]
        Impact_Cost = response.xpath('//table//tr[9]/td/text()').extract()
        if len(Impact_Cost) == 0:
            item["Impact_Cost"] = None
        else:
            item["Impact_Cost"] = Impact_Cost[0].replace("   ", "")
        FiftyTwo_week_high_or_low_price = response.xpath('//table//tr[10]/td/text()').extract()
        if len(FiftyTwo_week_high_or_low_price) == 0:
            item["FiftyTwo_week_high_or_low_price"] = None
        else:
            item["FiftyTwo_week_high_or_low_price"] = FiftyTwo_week_high_or_low_price[0]
        yield scrapy.Request(link, callback=self.parse2, meta={"item": item})

    def parse2(self, response):
        Purpose_list = []
        item = response.meta["item"]
        data_list = response.xpath('//table//tr')
        if len(data_list) != 0:
            del data_list[0]
            for temp in data_list:
                name = temp.xpath('./td/b/text()').extract()[0]
                value = temp.xpath('./td[4]/text()').extract()[0]
                data = name + " :" + value
                Purpose_list.append(data)
            item["Purpose"] = str(Purpose_list)
            yield item
        else:
            item["Purpose"] = None
            yield item
