# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


flag = 0
exclude_list = ["company_id", "name_origin","name_en","security_code","country_code_listed","exchange_market_code","ipo_date","currency_code","status","website_url","gmt_create","user_create","info_disclosure_id","ISIN"]


class IndiaPipeline(object):
    def is_update(self, jud_list, params, item, result, cursor, conn):
        name_origin = result[0]
        name_en = result[1]
        security_code = result[2]
        ipo_date = result[3]
        website_url = result[4]
        status = result[5]
        info_disclosure_id = result[6]
        isin = result[7]
        if item["name_origin"] is not None and item["name_origin"] != name_origin:
            jud_list.append("name_origin")
            params.append(item["name_origin"])
        if item["name_en"] is not None and item["name_en"] != name_en:
            jud_list.append("name_en")
            params.append(item["name_en"])
        if item["security_code"] is not None and item["security_code"] != security_code:
            jud_list.append("security_code")
            params.append(item["security_code"])
        if item["ipo_date"] is not None and str(item["ipo_date"]) != str(ipo_date):
            jud_list.append("ipo_date")
            params.append(item["ipo_date"])
        if item["website_url"] is not None and item["website_url"] != website_url:
            jud_list.append("website_url")
            params.append(item["website_url"])
        if item["statrs"] is not None and item["status"] != status:
            jud_list.append("status")
            params.append(item["status"])
        if item["ISIN"] is not None and item["ISIN"] != isin:
            jud_list.append("isin")
            params.append(item["ISIN"])
        if item["info_disclosure_id"] is not None and item["info_disclosure_id"] != info_disclosure_id:
            jud_list.append("info_disclosure_id")
            params.append(item["info_disclosure_id"])
        sql_update = "update company set "
        for temp in range(len(jud_list)):
            sql_update = sql_update + jud_list[temp] + "=%s" + ","
        sql_update = sql_update + " user_update=%s, gmt_update=%s where code = %s"
        params.extend(["zx", item["gmt_create"], item["company_id"]])
        cursor.execute(sql_update, params)
        conn.commit()

    def process_item(self, item, spider):
        conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        cursor = conn.cursor()
        jud_list = []
        params = []
        sql_select = "select name_origin,name_en,security_code,ipo_date,website_url,status,info_disclosure_id,isin from company where code = %s"
        cursor.execute(sql_select, item["company_id"])
        result = cursor.fetchone()
        print("%s,%s" % (item["company_id"], result))
        self.is_update(jud_list, params, item, result, cursor, conn)
        for each in item:
            if each not in exclude_list:
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
        conn.close()
        cursor.close()
        return item