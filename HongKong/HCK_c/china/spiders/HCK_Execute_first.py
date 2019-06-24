# # -*- coding: utf-8 -*-
# import os
# import pymysql
# import scrapy
# import xlrd
# import re
# import time
#
#
# class endPipeline(scrapy.Spider):
#     name = 'HCK_Execute_first'
#     allowed_domains = ['baidu.com']
#     start_urls = []
#     num = 0
#     max_company_num = 0
#     code_list_in_mysql = []
#     company_num_list = []
#     conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
#     cursor = conn.cursor()
#
#     def get_newest_company_file(self, name):
#         """获取最新的公司文件"""
#         name_list = []
#         name_num_list = []
#         dir_list = os.listdir("/data/OPDCMS/HCK/company_list")
#         for temp in dir_list:
#             if name in temp:
#                 name_title = str(temp).split(".")[0]
#                 name_list.append(name_title)
#         for each in name_list:
#             name_num = str(each).split("_")[-1]
#             name_num_list.append(int(name_num))
#         name_num_list.sort()
#         newest_name_num = str(name_num_list[-1])
#         return newest_name_num
#
#     def insert_new_data_to_mysql(self):
#         """插入新数据到company和company_data_source"""
#         newest_name_num = self.get_newest_company_file("HCK")
#         book = xlrd.open_workbook("/data/OPDCMS/HCK/company_list/HCK_" + newest_name_num + ".xls")
#         sheet = book.sheet_by_index(0)
#         for r in range(3, sheet.nrows):
#             Name_of_Securities = sheet.cell(r, 1).value
#             try:
#                 Par_Value = re.search("\w{3}", str(sheet.cell(r, 5).value)).group()
#             except:
#                 Par_Value = ""
#             ISIN = sheet.cell(r, 6).value
#             Stock_Code = sheet.cell(r, 0).value
#             jud_list = []
#             params = []
#             sql_select = "select name_origin,security_code from company where code = %s"
#             cursor.execute(sql_select, item["code"])
#             result = cursor.fetchone()
#             status = result[5]
#             if item["status"] != status:
#                 jud_list.append("status")
#                 params.append(item["status"])
#             self.is_update(jud_list, params, item, result, cursor, conn)
#             self.num += 1
#             print(self.num)
#         self.conn.close()
#         self.cursor.close()
#
#     def start_requests(self):
#         self.insert_new_data_to_mysql()
#         url = "http://www.baidu.com/"
#         yield scrapy.Request(url, callback=self.parse)
#
#     def parse(self, response):
#         print("The second part is completed！！！")
