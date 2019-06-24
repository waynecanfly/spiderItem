# -*- coding: utf-8 -*-
import os
import pymysql
import scrapy
import xlrd
import re
import time


class endPipeline(scrapy.Spider):
    name = 'HCK_Execute2'
    allowed_domains = ['baidu.com']
    start_urls = []
    num = 0
    max_company_num = 0
    code_list_in_mysql = []
    company_num_list = []
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()

    def get_newest_company_file(self, name):
        """获取最新的公司文件"""
        name_list = []
        name_num_list = []
        dir_list = os.listdir("/data/OPDCMS/HCK/company_list")
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

    def code_list_in_mysql_func(self):
        """获取数据库中还未获得的新上市的code list 以及数据库中最新的company_id"""
        sql = "select security_code, company_id from company_data_source where company_id like 'HKG%'"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for temp in results:
            code = temp[0]
            company_id = int(str(temp[1]).replace("HKG", ""))
            self.company_num_list.append(company_id)
            self.company_num_list.sort()
            self.code_list_in_mysql.append(code)
        self.max_company_num = self.company_num_list[-1]

    def insert_new_data_to_mysql(self):
        """插入新数据到company和company_data_source"""
        self.code_list_in_mysql_func()
        newest_name_num = self.get_newest_company_file("HCK")
        book = xlrd.open_workbook("/data/OPDCMS/HCK/company_list/HCK_" + newest_name_num + ".xls")
        sheet = book.sheet_by_index(0)
        for r in range(3, sheet.nrows):
            Name_of_Securities = sheet.cell(r, 1).value
            try:
                Par_Value = re.search("\w{3}", str(sheet.cell(r, 5).value)).group()
            except:
                Par_Value = ""
            ISIN = sheet.cell(r, 6).value
            Stock_Code = sheet.cell(r, 0).value
            if Stock_Code not in self.code_list_in_mysql:
                self.max_company_num += 1
                company_id = "HKG" + str(self.max_company_num)
                gmt_create = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                user_create = "zx"
                sql_jud = "select id from company where security_code=%s and country_code_listed=%s"
                self.cursor.execute(sql_jud, [Stock_Code, "HKG"])
                result = self.cursor.fetchone()
                if not result:
                    params1 = [
                        company_id, Stock_Code, Name_of_Securities, Par_Value, ISIN, gmt_create, user_create, "HKG", "HKEX"
                    ]
                    sql_company = "insert into company(code,security_code,name_origin,currency_code,isin,gmt_create," \
                                  "user_create,country_code_listed,exchange_market_code)value(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    self.cursor.execute(sql_company, params1)
                    self.conn.commit()
                params2 = [
                    company_id, Name_of_Securities, Stock_Code, 1, gmt_create, user_create,
                    "http://www.hkexnews.hk/listedco/listconews/advancedsearch/search_active_main.aspx", 0,
                    "HCK_pdf_spider"
                ]
                sql_data_source = "insert into company_data_source(company_id, company_name, security_code, is_batch, " \
                                  "gmt_create, user_create, download_link, mark, spider_name)values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql_data_source, params2)
                self.conn.commit()
                self.num += 1
                print(self.num)
        self.conn.close()
        self.cursor.close()

    def start_requests(self):
        self.insert_new_data_to_mysql()
        url = "http://www.baidu.com/"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        print("The second part is completed！！！")
