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
        args = {}
        for r in range(3, sheet.nrows):
            Name_of_Securities = sheet.cell(r, 1).value
            try:
                Par_Value = re.search("\w{3}", str(sheet.cell(r, 5).value)).group()
            except:
                Par_Value = ""
            ISIN = sheet.cell(r, 6).value
            Stock_Code = sheet.cell(r, 0).value
            Category = sheet.cell(r, 2).value
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
                        company_id, Stock_Code, Name_of_Securities, Par_Value, ISIN, gmt_create, user_create, "HKG", "HKEX", Category
                    ]
                    sql_company = "insert into company(code,security_code,name_origin,currency_code,isin,gmt_create," \
                                  "user_create,country_code_listed,exchange_market_code, security_type)value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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
                """插入数据到detail表"""
                args["Category_HCK"] = sheet.cell(r, 2).value
                args["Sub_Category_HCK"] = sheet.cell(r, 3).value
                args["Board_Lot_HCK"] = sheet.cell(r, 4).value
                args["Expiry_Date_HCK"] = sheet.cell(r, 7).value
                args["Subject_to_Stamp_Duty_HCK"] = sheet.cell(r, 8).value
                args["Shortsell_Eligible_HCK"] = sheet.cell(r, 9).value
                args["CAS_Eligible_HCK"] = sheet.cell(r, 10).value
                args["VCM_Eligible_HCK"] = sheet.cell(r, 11).value
                args["Admitted_to_Stock_Options_HCK"] = sheet.cell(r, 12).value
                args["Admitted_to_Stock_Futures_HCK"] = sheet.cell(r, 13).value
                args["Admitted_to_CCASS_HCK"] = sheet.cell(r, 14).value
                args["ETF_Fund_Manager_HCK"] = sheet.cell(r, 15).value
                args["Debt_Securities_Board_Lot_HCK"] = sheet.cell(r, 16).value
                args["Debt_Securities_Investor_Type_HCK"] = sheet.cell(r, 17).value
                gmt_create = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                user_create = "zx"
                for each in args:
                    if each == "Category_HCK":
                        id = "2861"
                    elif each == "Sub_Category_HCK":
                        id = "2862"
                    elif each == "Board_Lot_HCK":
                        id = "2863"
                    elif each == "Expiry_Date_HCK":
                        id = "2864"
                    elif each == "Subject_to_Stamp_Duty_HCK":
                        id = "2865"
                    elif each == "Shortsell_Eligible_HCK":
                        id = "2866"
                    elif each == "CAS_Eligible_HCK":
                        id = "2867"
                    elif each == "VCM_Eligible_HCK":
                        id = "2868"
                    elif each == "Admitted_to_Stock_Options_HCK":
                        id = "2869"
                    elif each == "Admitted_to_Stock_Futures_HCK":
                        id = "2870"
                    elif each == "Admitted_to_CCASS_HCK":
                        id = "2871"
                    elif each == "ETF_Fund_Manager_HCK":
                        id = "2872"
                    elif each == "Debt_Securities_Board_Lot_HCK":
                        id = "2873"
                    else:
                        id = "2874"
                    sql_detail_insert = "insert into company_profile_detail(company_code,company_profile_definition_id,value,gmt_create,user_create)value(%s,%s,%s,%s,%s)"
                    params2 = [
                        company_id,
                        id,
                        args[each],
                        gmt_create,
                        user_create
                    ]
                    self.cursor.execute(sql_detail_insert, params2)
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
