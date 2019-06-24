# -*- coding: utf-8 -*-
from scrapy import cmdline
import csv
import os
import pymysql
from scrapy.crawler import CrawlerProcess
from germany.ScriptDir.Move_2_Nas import Move2Nas
from germany.ScriptDir.Initialization import Initialization


num = 0
max_company_num = 0
code_list_in_mysql = []
new_code_list = []
code_list = []
company_num_list = []
process = CrawlerProcess()
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()


def get_newest_company_file():
    """获取最新的公司文件"""
    name_list = []
    name_num_list = []
    dir_list = os.listdir("D:\item\OPDCMS\listed company update\germany\data\company_list")
    for temp in dir_list:
        name_title = str(temp).split(".")[0]
        name_list.append(name_title)
    for each in name_list:
        name_num = str(each).split("_")[-1]
        name_num_list.append(int(name_num))
    name_num_list.sort()
    newest_name_num = str(name_num_list[-1])
    return newest_name_num


def get_shanghai_code_list():
    """获得ASX最新的code list"""
    newest_name_num = get_newest_company_file()
    excel_name = "D:\item\OPDCMS\listed company update\germany\data\company_list/frankfurt_" + newest_name_num + ".csv"
    f = open(excel_name, "r")
    reader = csv.reader(f)
    for i, row in enumerate(reader):
            isin = row[2]
            if isin not in code_list:
                code_list.append(isin)


def get_not_have_code_list():
    """获取数据库中还未获得的新上市的code list 以及数据库中最新的company_id"""
    global max_company_num
    get_shanghai_code_list()
    sql = "select security_code, company_id from company_data_source where company_id like 'DEU%'"
    cursor.execute(sql)
    results = cursor.fetchall()
    for temp in results:
        code = temp[0]
        company_id = int(str(temp[1]).replace("DEU1", ""))
        company_num_list.append(company_id)
        company_num_list.sort()
        code_list_in_mysql.append(code)
    for each in code_list:
        if each not in code_list_in_mysql:
            new_code_list.append(each)
    max_company_num = company_num_list[-1]


def insert_new_data_to_mysql():
    """插入新上市公司数据到company_data_source"""
    global max_company_num, num
    for temp in new_code_list:
        download_link = "http://en.boerse-frankfurt.de/search/advancedsharesearchresults?CountryID=1&IsPreferredStock=false&view=financial&p=1&count=25"
        max_company_num += 1
        if max_company_num < 1000:
            company_id = "DEU1" + "0" + str(max_company_num)
        else:
            company_id = "DEU1" + str(max_company_num)
        sql = "insert into company_data_source(company_id, security_code, mark, download_link) values(%s,%s,%s,%s)"
        cursor.execute(sql, [company_id, temp, 0, download_link])
        conn.commit()
        num += 1
        print(num)


def main():
    os.system("scrapy crawl FrankfurtCompanyList")
    get_not_have_code_list()
    insert_new_data_to_mysql()
    conn.close()
    cursor.close()
    os.system("scrapy crawl Frankfurtpdf")
    os.system("scrapy crawl FrankfurtBasicInfo")
    Initialization().InitializeMain()
    Move2Nas().Move2NasMain()


if __name__ == "__main__":
    main()
