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
    create_time = "20180425000000"
    exclusion_list = ["Sector", "doc_source_url", 'Web', 'company_id', 'info_disclosure_id', 'name_origin', "user_create", "gmt_create", "country_code_listed", "exchange_market_code", "currency_code"]

    def process_item(self, item, spider):
        conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        cursor = conn.cursor()
        jud_list = []
        params = []
        sql_select = "select name_origin,info_disclosure_id,industry,website_url from company where code = %s and code like %s"
        cursor.execute(sql_select, [item["company_id"], 'DEU%'])
        result = cursor.fetchone()
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
        cursor.execute(sql_update, params)
        conn.commit()

        for each in item:
            if each not in self.exclusion_list:
                sql_select = "select id from company_profile_definition where name = %s"
                cursor.execute(sql_select, each)
                result = cursor.fetchone()
                company_profile_definition_id = result[0]
                parameter_detail = [
                    item[each],
                    item["gmt_create"],
                    "zx",
                    item["company_id"],
                    company_profile_definition_id
                ]
                sql_jud = "select value from company_profile_detail where company_code = %s and company_profile_definition_id = %s"
                cursor.execute(sql_jud, [item["company_id"], company_profile_definition_id])
                jud_result = cursor.fetchone()
                if jud_result is not None:
                    if str(item[each]) != str(jud_result[0]):
                        sql_update = "update company_profile_detail set value = %s, gmt_update=%s, user_create=%s where company_code = %s and company_profile_definition_id = %s"
                        cursor.execute(sql_update, parameter_detail)
                        conn.commit()
        return item
