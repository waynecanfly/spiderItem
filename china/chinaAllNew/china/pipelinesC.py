# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import pymysql

from china.chinaAllNew.china.items import ChinaIntroItem_shanghai

flag = 0
flag2 = 0
newCodeList = []


class ChinaInfoPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def is_update(self, jud_list, params, item, result):
        name_origin = result[0]
        name_en = result[1]
        security_code = result[2]
        ipo_date = result[3]
        website_url = result[4]
        if item["name_origin"] != name_origin:
            jud_list.append("name_origin")
            params.append(item["name_origin"])
        if item["name_en"] != name_en:
            jud_list.append("name_en")
            params.append(item["name_en"])
        if item["security_code"] != security_code:
            jud_list.append("security_code")
            params.append(item["security_code"])
        if str(item["ipo_date"]) != str(ipo_date):
            jud_list.append("ipo_date")
            params.append(item["ipo_date"])
        if item["website_url"] != website_url:
            jud_list.append("website_url")
            params.append(item["website_url"])
        sql_update = "update company set "
        for temp in range(len(jud_list)):
            sql_update = sql_update + jud_list[temp] + "=%s" + ","
        sql_update = sql_update + " user_update=%s, gmt_update=%s where code = %s"
        params.extend(["zx", item["gmt_create"], item["code"]])
        self.cursor.execute(sql_update, params)
        self.conn.commit()

    def process_item(self, item, spider):
        # global flag2, newCodeList
        # if flag2 == 0:
        #     flag2 += 1
        #     sql_code = "select code from company where country_code_listed = 'CHN' and user_create = 'zx' order by gmt_update desc limit 1000"
        #     self.cursor.execute(sql_code)
        #     newCodeList = self.cursor.fetchall()
        #     newCodeList = [i[0] for i in newCodeList]
        if spider.name == "china_updateAll_c":
            spider_name = "china_updateAll_c"
            is_batch = 1
            if isinstance(item, ChinaIntroItem_shanghai):
                # if item["code"] in newCodeList:
                #     jud_list = []
                #     params = []
                #     sql_select = "select name_origin,name_en,security_code,ipo_date,website_url,status from company where code = %s"
                #     self.cursor.execute(sql_select, item["code"])
                #     result = self.cursor.fetchone()
                #     status = result[5]
                #     if item["status"] != status:
                #         jud_list.append("status")
                #         params.append(item["status"])
                #     self.is_update(jud_list, params, item, result)
                # else:
                download_link = "http://www.sse.com.cn/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=[code替换]"
                parameter_data_source = [
                    item["name_origin"],
                    download_link,
                    spider_name,
                    is_batch,
                    item["gmt_create"],
                    item["user_create"],
                    1,
                    item["code"]
                ]
                sql_data_source = "update company_data_source set company_name=%s, download_link=%s, spider_name=%s, is_batch=%s, gmt_create=%s, user_create=%s, mark=%s where company_id = %s"
                self.cursor.execute(sql_data_source, parameter_data_source)
                self.conn.commit()
                sql_jud = "select id from company where code = %s"
                self.cursor.execute(sql_jud, item["code"])
                results = self.cursor.fetchall()
                if len(results) == 0:
                    parameter_company = [
                        item["code"],
                        item["name_origin"],
                        item["name_en"],
                        item["security_code"],
                        item["country_code_listed"],
                        item["country_code_origin"],
                        item["exchange_market_code"],
                        item["ipo_date"],
                        item["currency_code"],
                        item["status"],
                        item["website_url"],
                        item["gmt_create"],
                        item["user_create"]
                    ]
                    sql_company = "insert into company(code,name_origin,name_en,security_code,country_code_listed,country_code_origin,exchange_market_code,ipo_date,currency_code,status,website_url,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    self.cursor.execute(sql_company, parameter_company)
                    self.conn.commit()

            if isinstance(item, ChinaIntroItem_shenzhen):
                # if item["code"] in newCodeList:
                #     jud_list = []
                #     params = []
                #     sql_select = "select name_origin,name_en,security_code,ipo_date,website_url from company where code = %s"
                #     self.cursor.execute(sql_select, item["code"])
                #     result = self.cursor.fetchone()
                #     self.is_update(jud_list, params, item, result)
                # else:
                download_link = "http://www.szse.cn/szseWeb/FrontController.szse?randnum=0.577506748569387"
                parameter_data_source = [
                    item["name_origin"],
                    download_link,
                    spider_name,
                    is_batch,
                    item["gmt_create"],
                    item["user_create"],
                    1,
                    item["code"]
                ]
                sql_data_source = "update company_data_source set company_name=%s, download_link=%s, spider_name=%s, is_batch=%s, gmt_create=%s, user_create=%s, mark=%s where company_id = %s"
                self.cursor.execute(sql_data_source, parameter_data_source)
                self.conn.commit()
                sql_jud = "select id from company where code = %s"
                self.cursor.execute(sql_jud, item["code"])
                results = self.cursor.fetchall()
                if len(results) == 0:
                    parameter_company = [
                        item["code"],
                        item["name_origin"],
                        item["name_en"],
                        item["security_code"],
                        item["country_code_listed"],
                        item["country_code_origin"],
                        item["exchange_market_code"],
                        item["ipo_date"],
                        item["currency_code"],
                        item["website_url"],
                        item["gmt_create"],
                        item["user_create"]
                    ]
                    sql_company = "insert into company(code,name_origin,name_en,security_code,country_code_listed,country_code_origin,exchange_market_code,ipo_date,currency_code,website_url,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    self.cursor.execute(sql_company, parameter_company)
                    self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()


class ChinaDetailPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "china_updateAll_c":
            global flag
            if flag == 0:
                flag += 1
                for temp in item:
                    if "_title" in temp:
                        name = str(temp).replace("_title", "") + "_CHN"
                        display_label = item[temp]
                        data_type = "string"
                        sort = 0
                        parameter = [
                            name,
                            display_label,
                            data_type,
                            sort,
                            item["gmt_create"],
                            "zx"
                        ]
                        sql_jud = "select id from company_profile_definition where name = %s"
                        self.cursor.execute(sql_jud, name)
                        results = self.cursor.fetchall()
                        if len(results) == 0:
                            sql = "insert into company_profile_definition(name,display_label,data_type,sort,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s)"
                            self.cursor.execute(sql, parameter)
                            self.conn.commit()
            for each in item:
                if "_title" in each:
                    name = str(each).replace("_title", "")
                    sql_select = "select id from company_profile_definition where name = %s"
                    self.cursor.execute(sql_select, name + "_CHN")
                    result = self.cursor.fetchone()
                    company_profile_definition_id = result[0]
                    parameter_detail = [
                        item[name],
                        item["gmt_create"],
                        "zx",
                        item["code"],
                        company_profile_definition_id
                    ]
                    sql_jud = "select value from company_profile_detail where company_code = %s and company_profile_definition_id = %s"
                    self.cursor.execute(sql_jud, [item["code"], company_profile_definition_id])
                    jud_result = self.cursor.fetchone()
                    if jud_result is not None:
                        if str(item[name]) != str(jud_result[0]):
                            sql_update = "update company_profile_detail set value = %s, gmt_update=%s, user_create=%s where company_code = %s and company_profile_definition_id = %s"
                            self.cursor.execute(sql_update, parameter_detail)
                            self.conn.commit()
                    else:
                        parameter_d = [
                            company_profile_definition_id,
                            item["code"],
                            item[name],
                            item["gmt_create"],
                            "zx"
                        ]
                        sql_insert = "insert into company_profile_detail(company_profile_definition_id,company_code,value,gmt_create,user_create)values(%s,%s,%s,%s,%s)"
                        self.cursor.execute(sql_insert, parameter_d)
                        self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()