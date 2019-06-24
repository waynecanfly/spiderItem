# -*- coding: utf-8 -*-
import scrapy
import csv
import os
import pymysql




class AsxSpider(scrapy.Spider):
    name = 'australiaExcuteFirst'
    allowed_domains = ['baidu.com']
    start_urls = []
    num = 0
    max_company_num = 0
    code_list_asx = []
    code_list_nsx = []
    code_list_in_mysql = []
    new_code_list_ASX = []
    new_code_list_NSX = []
    company_num_list = []
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA",charset="utf8")
    cursor = conn.cursor()

    def get_newest_company_file(self, name):
        """获取最新的公司文件"""
        name_list = []
        name_num_list = []
        dir_list = os.listdir("/data/OPDCMS/australia/listed_company_update/company_list")
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

    def get_shanghai_code_list(self):
        """获得ASX最新的code list"""
        newest_name_num = self.get_newest_company_file("ASX")
        excel_name = "/data/OPDCMS/australia/listed_company_update/company_list/ASX_" + newest_name_num + ".csv"
        f = open(excel_name, "r")
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= 3:
                company_name = row[0]
                code = row[1]
                industry = row[2]
                if code not in self.code_list_asx:
                    self.code_list_asx.append(code)

    def get_shenzhen_code_list(self):
        """获得NSX最新的code list"""
        newest_name_num = self.get_newest_company_file("NSX")
        excel_name = "/data/OPDCMS/australia/listed_company_update/company_list/NSX_" + newest_name_num + ".csv"
        f = open(excel_name, "r")
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= 1:
                code = row[1]
                if code not in self.code_list_nsx:
                    self.code_list_nsx.append(code)

    def get_not_have_code_list(self):
        """获取数据库中还未获得的新上市的code list 以及数据库中最新的company_id"""
        global max_company_num
        self.get_shanghai_code_list()
        self.get_shenzhen_code_list()
        sql = "select security_code, company_id from company_data_source where company_id like 'AUS%'"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for temp in results:
            code = temp[0]
            company_id = int(str(temp[1]).replace("AUS1", ""))
            self.company_num_list.append(company_id)
            self.company_num_list.sort()
            self.code_list_in_mysql.append(code)
        for each in self.code_list_asx:
            if each not in self.code_list_in_mysql:
                self.new_code_list_ASX.append(each)
        for each in self.code_list_nsx:
            if each not in self.code_list_in_mysql:
                self.new_code_list_NSX.append(each)
        max_company_num = self.company_num_list[-1]

    def insert_new_data_to_mysql(self, new_code_list, name):
        """插入新上市公司数据到company_data_source"""
        global max_company_num, num
        for temp in new_code_list:
            if name == "ASX":
                download_link = "https://www.asx.com.au/asx/research/listedCompanies.do"
            else:
                download_link = "https://www.nsx.com.au/marketdata/directory/"
            max_company_num += 1
            company_id = "AUS1" + str(max_company_num)
            sql = "insert into company_data_source(company_id, security_code, mark, download_link) values(%s,%s,%s,%s)"
            self.cursor.execute(sql, [company_id, temp, 0, download_link])
            self.conn.commit()
            self.num += 1
            print(self.num)

    def start_requests(self):
        from australia.australia_l.australia_l.DownloadCompanyList import DownloadCompanyList
        DownloadCompanyList("NSX").DownloadMain()
        DownloadCompanyList("ASX").DownloadMain()
        self.get_not_have_code_list()
        self.insert_new_data_to_mysql(self.new_code_list_ASX, "ASX")
        self.insert_new_data_to_mysql(self.new_code_list_NSX, "NSX")
        self.conn.close()
        self.cursor.close()
        url = "https://www.baidu.com/"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        print("The first part is completed！！！")
