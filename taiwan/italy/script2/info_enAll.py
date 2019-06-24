# -*- coding: utf-8 -*-
import time
import pymysql
import requests
from lxml import etree


num = 0
flag = 0
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()


def insertDetail(item, code):
    global flag, flag2
    if flag == 0:
        flag += 1
        for temp in item:
            parameter = [
                temp + "_TWN",
                temp,
                "string",
                0,
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                "zx"
            ]
            sql_jud = "select id from company_profile_definition where name = %s"
            cursor.execute(sql_jud, temp + "_TWN")
            results = cursor.fetchall()
            if len(results) == 0:
                sql = "insert into company_profile_definition(name,display_label,data_type,sort,gmt_create," \
                      "user_create)values(%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, parameter)
                conn.commit()
    for each in item:
        sql_select = "select id from company_profile_definition where name = %s"
        cursor.execute(sql_select, each + "_TWN")
        result = cursor.fetchone()
        company_profile_definition_id = result[0]
        sql_jud = "select value from company_profile_detail where company_code = %s and " \
                  "company_profile_definition_id = %s"
        cursor.execute(sql_jud, [code, company_profile_definition_id])
        jud_result = cursor.fetchone()
        if jud_result is None:
            parameter_d = [
                company_profile_definition_id,
                code,
                item[each],
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                "zx"
            ]
            sql_insert = "insert into company_profile_detail(company_profile_definition_id,company_code," \
                         "value,gmt_create,user_create)values(%s,%s,%s,%s,%s)"
            cursor.execute(sql_insert, parameter_d)
            conn.commit()


def insertBasic(item, company_code):
    update = [
        item["ipo_date"],
        item["Web_Address"],
        item["gmt_create"],
        item["user_create"],
        company_code
    ]
    sql_update = "update company set ipo_date=%s,website_url=%s,gmt_update=%s," \
                 "user_update=%s where code=%s"
    cursor.execute(sql_update, update)
    conn.commit()
    sql = "update company_data_source set mark = 1 where company_id=%s"
    cursor.execute(sql, company_code)
    conn.commit()


def parse(response):
    global num
    item = {}
    item2 = {}
    tr_list = response.xpath('//table[2]//tr')[1:]
    for temp in tr_list:
        security_code = temp.xpath('./td[2]/text()')[0]
        sql = "select company_id from company_data_source where security_code=%s and company_id like %s"
        cursor.execute(sql, [security_code, 'TWN%'])
        company_code = cursor.fetchone()
        if company_code:
            company_code = company_code[0]
            num += 1
            print(num)
            if num > 2085:
                if security_code == "000537" or security_code == "000736":
                    ipo_date = temp.xpath('./td[7]/text()')[0]
                    item["ipo_date"] = str(ipo_date).split("/")[0] + "-" + str(ipo_date).split("/")[1] + "-" + \
                                       str(ipo_date).split("/")[-1] + " 00:00:00"
                    item2["Place_of_Incorporation_of_Foreign_Companies"] = temp.xpath('./td[4]/text()')[0]
                    item2["Chairman"] = ""
                    item2["General_Manager"] = ""
                    item2["Spokesman"] = ""
                    item2["Title_of_Spokesman"] = ""
                    item2["Fiscal_Year_end"] = temp.xpath('./td[12]/text()')[0]
                    item2["Accounting_Firm"] = ""
                    item2["Chartered_Public_Accountant1"] = ""
                    item2["Chartered_Public_Accountant2"] = ""
                    item2["Address"] = temp.xpath('./td[16]/text()')[0]
                    item2["Telephone"] = temp.xpath('./td[17]/text()')[0]
                    item2["Fax"] = temp.xpath('./td[18]/text()')[0]
                    item2["Email_Address"] = temp.xpath('./td[19]/text()')[0]
                    item["Web_Address"] = temp.xpath('./td[20]/text()')[0]
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["user_create"] = "zx"
                else:
                    ipo_date = temp.xpath('./td[7]/text()')[0]
                    item["ipo_date"] = str(ipo_date).split("/")[0] + "-" + str(ipo_date).split("/")[1] + "-" + str(ipo_date).split("/")[-1] + " 00:00:00"
                    item2["Place_of_Incorporation_of_Foreign_Companies"] = str(temp.xpath('./td[4]/text()')[0]).replace("¡Ð", "-")
                    item2["Chairman"] = temp.xpath('./td[8]/text()')[0]
                    item2["General_Manager"] = temp.xpath('./td[9]/text()')[0]
                    item2["Spokesman"] = temp.xpath('./td[10]/text()')[0]
                    item2["Title_of_Spokesman"] = temp.xpath('./td[11]/text()')[0]
                    item2["Fiscal_Year_end"] = temp.xpath('./td[12]/text()')[0]
                    item2["Accounting_Firm"] = temp.xpath('./td[13]/text()')[0]
                    item2["Chartered_Public_Accountant1"] = temp.xpath('./td[14]/text()')[0]
                    item2["Chartered_Public_Accountant2"] = temp.xpath('./td[15]/text()')[0]
                    item2["Address"] = str(temp.xpath('./td[16]/text()')).replace("\n", "").replace("'", "").replace("[", "").replace("]", "")
                    item2["Telephone"] = temp.xpath('./td[17]/text()')[0]
                    item2["Fax"] = temp.xpath('./td[18]/text()')[0]
                    item2["Email_Address"] = temp.xpath('./td[19]/text()')[0]
                    item["Web_Address"] = temp.xpath('./td[20]/text()')[0]
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["user_create"] = "zx"
                insertBasic(item, company_code)
                insertDetail(item2, company_code)


response = requests.get("http://emops.twse.com.tw/server-java/t51sb01_e")
parse(etree.HTML(response.text))