# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
import time
from datetime import datetime, timedelta
from italy.items import ItalydetailItem, ItalyCompanyItem, ItalyfileItem


flag = 0
flag2 = 0
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
with open("/root/spiderItem/SouthAfrica/record.txt", "r") as f:
    data = f.read()
    year = int(data.split("@")[0])
    month = int(data.split("@")[1])
    day = int(data.split("@")[-1])
    a = datetime(year, month, day) + timedelta(days=30)

sql_max = "select max(company_id) from company_data_source where company_id like 'ZAF%'"
cursor.execute(sql_max)
newstCode = cursor.fetchone()
if newstCode[0]:
    newstNum = int(str(newstCode[0]).replace("ZAF", ""))
else:
    newstNum = 10000


class ItalyPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        global a, flag
        if isinstance(item, ItalyCompanyItem):
            sql = "select company_id from company_data_source where security_code=%s and company_id like %s"
            self.cursor.execute(sql, [item["security_code"], 'ZAF%'])
            result = self.cursor.fetchone()
            if result:
                if a <= datetime.now():
                    if flag == 0:
                        flag += 1
                        newRecord = str(datetime.now()).split(" ")[0].split("-")[0] + "@" + \
                                    str(datetime.now()).split(" ")[0].split("-")[1] + "@" + \
                                    str(datetime.now()).split(" ")[0].split("-")[-1]
                        with open("/root/spiderItem/SouthAfrica/record.txt", "w") as g:
                            g.write(newRecord)
                    update = [
                        item["name"],
                        item["Website"],
                        item["MasterID"],
                        item["ISIN"],
                        item["Industry"],
                        item["Sector"],
                        item["gmt_create"],
                        item["user_create"],
                        result[0],
                        item["security_code"],
                    ]
                    sql_update = "update company set name_origin=%s,website_url=%s,info_disclosure_id=%s,isin=%s," \
                                 "industry=%s,sector_code=%s,gmt_update=%s,user_update=%s where code=%s and security_code=%s"
                    self.cursor.execute(sql_update, update)
                    self.conn.commit()
                else:
                    pass
            else:
                global newstNum
                newstNum += 1
                newCode = "ZAF" + str(newstNum)

                #插入数据到comapany_data_source
                is_batch = 1
                download_link = "https://www.jse.co.za"
                parameter_data_source = [
                    item["name"],
                    download_link,
                    "sfAll",
                    is_batch,
                    item["gmt_create"],
                    item["user_create"],
                    1,
                    newCode,
                    item["security_code"]
                ]
                sql_data_source = "insert into company_data_source(company_name,download_link,spider_name,is_batch," \
                                  "gmt_create,user_create,mark,company_id,security_code)value(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql_data_source, parameter_data_source)
                self.conn.commit()
                #插入数据到company表
                params = [
                    newCode,
                    item["name"],
                    item["security_code"],
                    item["Website"],
                    item["MasterID"],
                    item["country_code_listed"],
                    item["exchange_market_code"],
                    item["gmt_create"],
                    item["user_create"],
                    item["ISIN"],
                    item["Industry"],
                    item["Sector"],
                ]
                company_insert = "insert into company(code,name_origin,security_code,website_url,info_disclosure_id," \
                                 "country_code_listed,exchange_market_code,gmt_create,user_create,isin,industry," \
                                 "sector_code)value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(company_insert, params)
                self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()


class ItalyDetailPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if isinstance(item, ItalydetailItem):
            sql = "select company_id from company_data_source where security_code=%s and company_id like %s"
            self.cursor.execute(sql, [item["security_code"], 'ZAF%'])
            code = self.cursor.fetchone()[0]
            del item["security_code"]
            global flag2, a
            if flag2 == 0:
                flag2 += 1
                for temp in item:
                    parameter = [
                        temp + "_ZAF",
                        temp,
                        "string",
                        0,
                        time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                        "zx"
                    ]
                    sql_jud = "select id from company_profile_definition where name = %s"
                    self.cursor.execute(sql_jud, temp + "_ZAF")
                    results = self.cursor.fetchall()
                    if len(results) == 0:
                        sql = "insert into company_profile_definition(name,display_label,data_type,sort,gmt_create," \
                              "user_create)values(%s,%s,%s,%s,%s,%s)"
                        self.cursor.execute(sql, parameter)
                        self.conn.commit()
            for each in item:
                sql_select = "select id from company_profile_definition where name = %s"
                self.cursor.execute(sql_select, each + "_ZAF")
                result = self.cursor.fetchone()
                company_profile_definition_id = result[0]
                parameter_detail = [
                    item[each],
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                    "zx",
                    code,
                    company_profile_definition_id
                ]
                sql_jud = "select value from company_profile_detail where company_code = %s and " \
                          "company_profile_definition_id = %s"
                self.cursor.execute(sql_jud, [code, company_profile_definition_id])
                jud_result = self.cursor.fetchone()
                if jud_result is not None and a <= datetime.now():
                    if str(item[each]) != str(jud_result[0]):
                        sql_update = "update company_profile_detail set value = %s, gmt_update=%s, user_create=%s " \
                                     "where company_code = %s and company_profile_definition_id = %s"
                        self.cursor.execute(sql_update, parameter_detail)
                        self.conn.commit()
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
                    self.cursor.execute(sql_insert, parameter_d)
                    self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()


class southAfracaFilePiplne(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if isinstance(item, ItalyfileItem):
            params = [
                item["country_code"],
                item["exchange_market_code"],
                item["company_code"],
                item["fiscal_year"],
                item["financial_statement_season_type_code"],
                item["disclosure_date"],
                item["pdf_name"],
                item["language_written_code"],
                item["doc_type"],
                item["doc_source_url"],
                item["is_doc_url_direct"],
                item["doc_local_path"],
                item["doc_downloaded_timestamp"],
                item["is_downloaded"],
                item["gmt_create"],
                item["user_create"],
                item["report_id"]
            ]
            sql = "insert into financial_statement_index(country_code,exchange_market_code,company_code,fiscal_year," \
                  "financial_statement_season_type_code,disclosure_date,file_original_title,language_written_code," \
                  "doc_type,doc_source_url,is_doc_url_direct,doc_local_path,doc_downloaded_timestamp,is_downloaded," \
                  "gmt_create,user_create,report_id)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql, params)
            self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()