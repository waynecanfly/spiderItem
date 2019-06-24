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
    url = "https://www.bseindia.com/stock-share-price/corp-information/scripcode/"
    code_list = []
    company_id_list = []
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    # sql = "select security_code,company_id from company_data_source where mark = 0 and company_id like " + "'IND%' and spider_name = 'BasicInfoBSE'"
    sql = "select security_code,company_id from company_data_source where company_id like " + "'IND%' and spider_name = 'BasicInfoBSE'"
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
        newest_name_num = self.get_newest_company_file("BSE")
        excel_name = "/data/OPDCMS/india/listed_company_update/company_list/BSE_" + newest_name_num + ".csv"
        f = open(excel_name, "r")
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= 1:
                code = row[0]
                if code in self.code_list:
                    item = IndiaItem()
                    item["security_code"] = code
                    iNum = self.code_list.index(code)
                    item["company_id"] = self.company_id_list[iNum]
                    item["Symbol"] = row[1]
                    item["name_origin"] = row[2]
                    item["name_en"] = item["name_origin"]
                    item["status"] = row[3]
                    item["Group_Num"] = row[4]
                    item["Face_Value"] = row[5]
                    item["ISIN"] = row[6]
                    item["Industry"] = row[7]
                    item["Instrument"] = row[8]
                    item["country_code_listed"] = "IND"
                    item["exchange_market_code"] = "BSE"
                    item["currency_code"] = "INR"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["user_create"] = "zx"
                    item["info_disclosure_id"] = None
                    url = self.url + str(code) + "/"
                    yield scrapy.Request(url, callback=self.parse, meta={"item": item})

    def parse(self, response):
        item = response.meta["item"]
        management_list = []
        CIN = response.xpath('//table//tr/td/strong[text()="CIN"]/parent::td/following-sibling::td/text()').extract()
        if len(CIN) == 0:
            item["CIN"] = None
        else:
            item["CIN"] = CIN[0]
        Impact_Cost = response.xpath('//table//tr/td/strong[text()="Impact Cost"]/parent::td/following-sibling::td/text()').extract()
        if len(Impact_Cost) == 0:
            item["Impact_Cost"] = None
        else:
            item["Impact_Cost"] = Impact_Cost[0]
        a = response.xpath('//table//tr/td/strong[text()="BC/RD"]/parent::td/following-sibling::td/text()').extract()
        if len(a) == 0:
            item["BC_RD"] = None
        else:
            item["BC_RD"] = a[0]
        b = response.xpath('//table//tr/td/strong[text()="Market lot"]/parent::td/following-sibling::td/text()').extract()
        if len(b) == 0:
            item["Market_Lot"] = None
        else:
            item["Market_Lot"] = b[0]
        try:
            data = response.xpath(
                '//table//tr/td/strong[text()="Listing Date"]/parent::td/following-sibling::td/text()').extract()[0]
            item["ipo_date"] = str(data).split("-")[-1] + "-" + str(data).split("-")[1] + "-" + str(data).split("-")[0] + " 00:00:00"
        except:
            item["ipo_date"] = None
        c = response.xpath('//table//tr/td/strong[text()="Tel. :"]/parent::td/following-sibling::td/text()').extract()
        if len(c) == 0:
            item["Registered_Office_Tel"] = None
        else:
            item["Registered_Office_Tel"] = c[0]
        d = response.xpath('//table//tr/td/strong[text()="Fax. :"]/parent::td/following-sibling::td/text()').extract()
        if len(d) == 0:
            item["Registered_Office_Fax"] = None
        else:
            item["Registered_Office_Fax"] = d[0]
        e = response.xpath('//table//tr/td/strong[text()="Email :"]/parent::td/following-sibling::td/a/@href').extract()
        if len(e) == 0:
            item["Registered_Office_Email"] = None
        else:
            item["Registered_Office_Email"] = e[0]
        f = response.xpath('//table//tr/td/strong[text()="Website :"]/parent::td/following-sibling::td/a/@href').extract()
        if len(f) == 0:
            item["website_url"] = None
        else:
            item["website_url"] = f[0]
        g = response.xpath('//table//tr/td/strong[text()="Tel. :"]/parent::td/following-sibling::td/text()').extract()
        if len(g) == 0:
            item["Registrars_Tel"] = None
        else:
            item["Registrars_Tel"] = g[-1]
        h = response.xpath('//table//tr/td/strong[text()="Fax. :"]/parent::td/following-sibling::td/text()').extract()
        if len(h) == 0:
            item["Registrars_Fax"] = None
        else:
            item["Registrars_Fax"] = h[-1]
        i = response.xpath('//table//tr/td/strong[text()="Email :"]/parent::td/following-sibling::td/a/@href').extract()
        if len(i) == 0:
            item["Registrars_Email"] = None
        else:
            item["Registrars_Email"] = i[-1]
        j = response.xpath('//table//tr/td/strong[text()="Website :"]/parent::td/following-sibling::td/a/@href').extract()
        if len(j) == 0:
            item["Registrars_Website"] = None
        else:
            item["Registrars_Website"] = j[-1]
        manage_list = response.xpath('//table//tr/td[text()="Designation"]/parent::tr/following-sibling::tr')
        for temp in manage_list:
            Manage_Name = str(temp.xpath('./td[1]/text()').extract()[0]).replace("&nbsp", " ").replace("  ", "")
            Designation = temp.xpath('./td[2]/text()').extract()[0]
            data = Manage_Name + ": " + Designation
            management_list.append(data)
        item["management_list"] = str(management_list)
        yield item
