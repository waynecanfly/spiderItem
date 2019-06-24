# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import time
import pymysql


class GermanyPipeline(object):
    flag = 0
    flag2 = 0
    flag3 = 0
    create_time = "20180425000000"
    exclusion_list = ["Sector", "doc_source_url", 'Web', 'company_id', 'info_disclosure_id', 'name_origin', "user_create", "gmt_create", "country_code_listed", "exchange_market_code", "currency_code"]

    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        jud_list = []
        params = []
        sql_select = "select name_origin,info_disclosure_id,industry,website_url from company where code = %s and code like %s"
        self.cursor.execute(sql_select, [item["company_id"], 'DEU%'])
        result = self.cursor.fetchone()
        name_origin = result[0]
        info_disclosure_id = result[1]
        industry = result[2]
        website_url = result[3]
        if item["name_origin"] != name_origin and item["name_origin"] is not None:
            jud_list.append("name_origin")
            params.append(item["name_origin"])
        if item["info_disclosure_id"] != info_disclosure_id and item["info_disclosure_id"] is not None:
            jud_list.append("info_disclosure_id")
            params.append(item["info_disclosure_id"])
        if str(item["Sector"]) != str(industry) and item["Sector"] is not None and item["Sector"] != "-":
            jud_list.append("industry")
            params.append(item["Sector"])
        if item["Web"] != website_url and item["Web"] is not None:
            jud_list.append("website_url")
            params.append(item["Web"])
        sql_update = "update company set "
        for temp in range(len(jud_list)):
            sql_update = sql_update + jud_list[temp] + "=%s" + ","
        sql_update = sql_update + " user_update=%s, gmt_update=%s where code = %s"
        params.extend(["zx", item["gmt_create"], item["company_id"]])
        self.cursor.execute(sql_update, params)
        self.conn.commit()

        if self.flag3 == 0:
            self.flag3 += 1
            for temp in item:
                if temp not in self.exclusion_list:
                    name = temp + "_DEU"
                    display_label = temp
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
            if each not in self.exclusion_list:
                sql_select = "select id from company_profile_definition where name = %s"
                self.cursor.execute(sql_select, each + "_DEU")
                result = self.cursor.fetchone()
                company_profile_definition_id = result[0]
                parameter_detail = [
                    item[each],
                    item["gmt_create"],
                    "zx",
                    item["company_id"],
                    company_profile_definition_id
                ]
                sql_jud = "select value from company_profile_detail where company_code = %s and company_profile_definition_id = %s"
                self.cursor.execute(sql_jud, [item["company_id"], company_profile_definition_id])
                jud_result = self.cursor.fetchone()
                if jud_result is not None:
                    if str(item[each]) != str(jud_result[0]):
                        sql_update = "update company_profile_detail set value = %s, gmt_update=%s, user_create=%s where company_code = %s and company_profile_definition_id = %s"
                        self.cursor.execute(sql_update, parameter_detail)
                        self.conn.commit()
                else:
                    sql_insert = "insert into company_profile_detail(value,gmt_create,user_create,company_code,company_profile_definition_id)values(%s,%s,%s,%s,%s)"
                    self.cursor.execute(sql_insert, parameter_detail)
                    self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()