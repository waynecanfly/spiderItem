# -*- coding: utf-8 -*-
import os
import re
import pymysql
import scrapy
from china.ScriptDir.UpdatePro_financial_statement_index import UpdateFinancialStatementIndex
from china.ScriptDir.Move_2_Nas import Move2Nas
from china.ScriptDir.Initialization import Initialization
from china.ScriptDir.unzip_download_files import Decompression
from china.ScriptDir.Download_Financial_Statements import DownloadFinancialStatements


class endPipeline(object):
    num = 0
    max_company_num = 0
    code_list_shanghai = []
    code_list_shenzhen = []
    code_list_all = []
    code_list_in_mysql = []
    new_code_list = []
    company_num_list = []
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()

    def get_newest_company_file(name):
        """获取最新的公司文件"""
        name_list = []
        name_num_list = []
        dir_list = os.listdir("D:\item\OPDCMS\listed company update\china\data\company_list")
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
        """获得上交所最新的code list"""
        newest_name_num = self.get_newest_company_file("shanghai")
        with open("D:\item\OPDCMS\listed company update\china\data\company_list/shanghai_" + newest_name_num + ".txt") as f:
            data = f.read()
        pattern = re.compile('(60\d{4})\s')
        code = pattern.findall(data)
        for temp in code:
            if temp not in self.code_list_shanghai:
                self.code_list_shanghai.append(temp)
                # num = len(code_list_shanghai)
                # print(num)

    def get_shenzhen_code_list(self):
        """获得深交所最新的code list"""
        newest_name_num = self.get_newest_company_file("shenzhen")
        with open("D:\item\OPDCMS\listed company update\china\data\company_list/shenzhen_" + newest_name_num + ".txt") as f:
            data = f.read()
        code = data.split(",")
        del code[-1]
        for each in code:
            if each not in self.code_list_shenzhen:
                self.code_list_shenzhen.append(each)

    def get_all_code_list(self):
        """获得深交所和深交所最新的code list"""
        self.get_shanghai_code_list()
        self.get_shenzhen_code_list()
        self.code_list_all.extend(self.code_list_shanghai)
        self.code_list_all.extend(self.code_list_shenzhen)

    def get_not_have_code_list(self):
        """获取数据库中还未获得的新上市的code list 以及数据库中最新的company_id"""
        self.get_all_code_list()
        sql = "select security_code, company_id from company_data_source where company_id like 'CHN%'"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for temp in results:
            code = temp[0]
            company_id = int(str(temp[1]).replace("CHN1", ""))
            self.company_num_list.append(company_id)
            self.company_num_list.sort()
            self.code_list_in_mysql.append(code)
        for each in self.code_list_all:
            if each not in self.code_list_in_mysql:
                self.new_code_list.append(each)
        self.max_company_num = self.company_num_list[-1]

    def insert_new_data_to_mysql(self):
        """插入新上市公司数据到company_data_source"""
        for temp in self.new_code_list:
            self.max_company_num += 1
            company_id = "CHN1" + str(self.max_company_num)
            """#直接是与company_data_source对比拿到新上市公司列表的，所以不会存在重复问题
            sql_jud = "select id from company_data_source where security_code = %s and company_id like %s"
            cursor.execute(sql_jud, [temp, 'CHN%'])
            result = cursor.fetchall()
            if len(result) == 0:
            """
            sql = "insert into company_data_source(company_id, security_code, mark) values(%s,%s,%s)"
            self.cursor.execute(sql, [company_id, temp, 0])
            #self.conn.commit()
            self.num += 1
            print(self.num)
        self.conn.close()
        self.cursor.close()

    def process_item(self, item, spider):
        if spider.name == "UpdatePro_shanghai":
            os.system("scrapy crawl UpdatePro_shanghai")
            os.system("scrapy crawl UpdatePro_shenzhen")
            self.get_not_have_code_list()
            self.insert_new_data_to_mysql()
            if self.num > 0:
                os.system("scrapy crawl shenzhen_download_spider")
                os.system("scrapy crawl shanghai_download_spider")
                os.system("scrapy crawl UpdatePro_shenzhen_basicinfo")
                os.system("scrapy crawl UpdatePro_shanghai_basicinfo")
                Initialization().InitializeMain()
                UpdateFinancialStatementIndex().UpdateReportMain()
                Move2Nas().Move2NasMain()
