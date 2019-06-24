# -*- coding: utf-8 -*-
import scrapy
import os
import pymysql
from india.ScriptDir.Initialization import Initialization
from india.ScriptDir.unzip_download_files import Decompression
from india.ScriptDir.Move_2_Nas import Move2Nas


num = 0
flag = None
excel_name_bse = None
excel_name_nse = None
max_company_num = 0
code_list_bse = []
symbol_list_bse = []
symbol_list_nse = []
code_list_all = []
code_list_in_mysql = []
symbol_list_in_mysql = []
new_code_list = []
go_heavy_symbol_list = []
new_symbol_list = []
company_num_list = []
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()


def get_newest_company_file(name):
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


def get_bse_code_list():
    """获得BSE最新的code list"""
    global flag, excel_name_bse
    newest_name_num = get_newest_company_file("BSE")
    excel_name_bse = "D:\item\OPDCMS\listed company update\india\data\companyList/BSE_" + newest_name_num + ".csv"
    with open(excel_name_bse, "r") as f:
        data = f.readlines()
        for temp in data:
            if flag:
                code = temp.split(",")[0]
                symbol = temp.split(",")[1]
                if code not in code_list_bse:
                    code_list_bse.append(code)
                if symbol not in symbol_list_bse:
                    symbol_list_bse.append(symbol)
            flag = True
        flag = None


def get_nse_code_list():
    """获得NSE最新的code list"""
    global flag, excel_name_nse
    newest_name_num = get_newest_company_file("NSE")
    excel_name_nse = "D:\item\OPDCMS\listed company update\india\data\companyList/NSE_" + newest_name_num + ".csv"
    with open(excel_name_nse, "r") as f:
        data = f.readlines()
        for temp in data:
            if flag:
                symbol = temp.split(",")[0].replace('"', "")
                if symbol not in symbol_list_nse:
                    symbol_list_nse.append(symbol)
            else:
                flag = True
        flag = None


def get_all_code_list():
    """获得NSE和BSE最新的code list"""
    get_bse_code_list()
    get_nse_code_list()
    for temp in symbol_list_nse:
        if temp not in symbol_list_bse:
            go_heavy_symbol_list.append(temp)


def get_not_have_code_list():
    """获取数据库中还未获得的新上市的code list 以及数据库中最新的company_id"""
    global max_company_num
    get_all_code_list()
    sql = "select security_code, company_id, info_disclosure_id from company_data_source where company_id like 'IND%'"
    cursor.execute(sql)
    results = cursor.fetchall()
    for temp in results:
        code = temp[0]
        company_id = int(str(temp[1]).replace("IND1", ""))
        info_disclosure_id = temp[2]
        company_num_list.append(company_id)
        company_num_list.sort()
        code_list_in_mysql.append(code)
        symbol_list_in_mysql.append(info_disclosure_id)
    for each in code_list_bse:
        if each not in code_list_in_mysql:
            new_code_list.append(each)
    for eachOne in go_heavy_symbol_list:
        if eachOne not in symbol_list_in_mysql:
            new_symbol_list.append(eachOne)
    max_company_num = company_num_list[-1]


def insert_new_data_to_mysql(sList, param, name):
    """插入新上市公司数据到company_data_source"""
    global max_company_num, num, excel_name_bse, excel_name_nse
    if name == "bse":
        f = open(excel_name_bse, "r")
    else:
        f = open(excel_name_nse, "r")
    data = f.readlines()
    for temp in data:
        if name == "bse":
            code = temp.split(",")[0]
        else:
            code = temp.split(",")[0].replace('"', "")
        if code in sList:
            if name == "bse":
                company_name = temp.split(",")[2]
                tVlaue = temp.split(",")[1]
                download_link = "https://www.bseindia.com/corporates/List_Scrips.aspx?expandable=1"
                spiderName = "BasicInfoBSE"
                tag = "info_disclosure_id"
            else:
                tVlaue = None
                company_name = temp.split(",")[2].replace('"', "")
                download_link = "https://www.nseindia.com/corporates/corporateHome.html?id=equity"
                spiderName = "BasicInfoNSE"
                tag = "security_code"
            max_company_num += 1
            company_id = "IND1" + str(max_company_num)
            sql = "insert into company_data_source(company_id, " + param + ", " + tag + ", mark, company_name, download_link, spider_name) values(%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, [company_id, code, tVlaue, 0, company_name, download_link, spiderName])
            conn.commit()
            num += 1
            print(num)


def main():
    os.system("scrapy crawl companylist_BSE")
    os.system("scrapy crawl companylist_NSE")
    get_not_have_code_list()
    insert_new_data_to_mysql(new_code_list, "security_code", "bse")
    insert_new_data_to_mysql(new_symbol_list, "info_disclosure_id", "nse")
    os.system("scrapy crawl downloadExcel_BSE")
    os.system("scrapy crawl downloadPdf_BSE_A")
    os.system("scrapy crawl downloadPdf_BSE_Q")
    os.system("scrapy crawl downloadExcel_NSE")
    os.system("scrapy crawl downloadZip_NSE")
    os.system("scrapy crawl BasicInfoBSE")
    os.system("scrapy crawl BasicInfoNSE")
    Initialization().InitializeMain()
    Decompression().UnzipMain()
    Move2Nas.Move2NasMain()

if __name__ == "__main__":
    main()
