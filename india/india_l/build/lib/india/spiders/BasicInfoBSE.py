# -*- coding: utf-8 -*-
import scrapy
import csv
import os
import pymysql
import time
from india.items import IndiaItem


class IndiaSpider(scrapy.Spider):
    name = 'BasicInfoBSE'
    allowed_domains = ['bseindia.com']
    url1 = "http://www.bseindia.com/stock-share-price/stockreach_corpinfo.aspx?scripcode="
    url2 = "&expandable=8"
    code_list = []
    company_id_list = []
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,company_id from company_data_source where mark = 0 and company_id like " + "'IND%' and spider_name = 'BasicInfoBSE'"
    cursor.execute(sql)
    results = cursor.fetchall()
    for temp in results:
        code_list.append(temp[0])
        company_id_list.append(temp[1])

    def get_newest_company_file(self, name):
        """获取最新的公司文件"""
        name_list = []
        name_num_list = []
        dir_list = os.listdir("D:\item\OPDCMS\listed company update\india\data\companyList")
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
        newest_name_num = self.get_newest_company_file("BSE")
        excel_name = "D:\item\OPDCMS\listed company update\india\data\companyList/BSE_" + newest_name_num + ".csv"
        f = open(excel_name, "r")
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= 1:
                code = row[0].decode("gbk").encode("utf-8")
                if code in self.code_list:
                    item = IndiaItem()
                    item["security_code"] = code
                    iNum = self.code_list.index(code)
                    item["company_id"] = self.company_id_list[iNum]
                    item["Symbol"] = row[1].decode("gbk").encode("utf-8")
                    item["name_origin"] = row[2].decode("gbk").encode("utf-8")
                    item["name_en"] = item["name_origin"]
                    item["status"] = row[3].decode("gbk").encode("utf-8")
                    item["Group_Num"] = row[4].decode("gbk").encode("utf-8")
                    item["Face_Value"] = row[5].decode("gbk").encode("utf-8")
                    item["ISIN"] = row[6].decode("gbk").encode("utf-8")
                    item["Industry"] = row[7].decode("gbk").encode("utf-8")
                    item["Instrument"] = row[8].decode("gbk").encode("utf-8")
                    item["country_code_listed"] = "IND"
                    item["exchange_market_code"] = "BSE"
                    item["currency_code"] = "INR"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["user_create"] = "root"
                    item["info_disclosure_id"] = None
                    url = self.url1 + str(code) + self.url2
                    yield scrapy.Request(url, callback=self.parse, meta={"item": item})

    def parse(self, response):
        item = response.meta["item"]
        management_list = []
        item["CIN"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_scripdetailsdiv"]//tr[3]/td/text()').extract()[0]
        item["Impact_Cost"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_scripdetailsdiv"]//tr[5]/td/text()').extract()[0]
        item["BC_RD"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_scripdetailsdiv"]//tr[6]/td/text()').extract()[0]
        item["Market_Lot"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_scripdetailsdiv"]//tr[7]/td/text()').extract()[0]
        data = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_scripdetailsdiv"]//tr[8]/td/text()').extract()[0]
        try:
            item["ipo_date"] = str(data).split("-")[-1] + "-" + str(data).split("-")[1] + "-" + str(data).split("-")[0] + " 00:00:00"
        except:
            item["ipo_date"] = None
        item["Registered_Office_Tel"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_managementdiv"]//tr[3]/td[2]/text()').extract()[1]
        item["Registered_Office_Fax"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_managementdiv"]//tr[4]/td[2]/text()').extract()[1]
        item["Registered_Office_Email"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_managementdiv"]//tr[5]/td[2]/a/@href').extract()[0]
        item["website_url"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_managementdiv"]//tr[6]/td[2]/a/@href').extract()[0]
        item["Registrars_Tel"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_managementdiv"]//tr[3]/td[2]/text()').extract()[-1]
        item["Registrars_Fax"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_managementdiv"]//tr[4]/td[2]/text()').extract()[-1]
        item["Registrars_Email"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_managementdiv"]//tr[5]/td[2]/a/@href').extract()[1]
        item["Registrars_Website"] = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_managementdiv"]//tr[6]/td[2]/a/@href').extract()[1]
        manage_list = response.xpath('//div[@id="ctl00_ContentPlaceHolder1_managementdiv"]//table[1]//table//tr')
        del manage_list[:2]
        for temp in manage_list:
            Manage_Name = str(temp.xpath('./td[1]/text()').extract()[0]).replace("&nbsp", " ").replace("  ", "")
            Designation = temp.xpath('./td[2]/text()').extract()[0]
            data = Manage_Name + ": " + Designation
            management_list.append(data)
        item["management_list"] = str(management_list)
        yield item
