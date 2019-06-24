# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


flag = 0


class ChinaDetailPipeline(object):
    def process_item(self, item, spider):
        if spider.name == "HCK_information":
            global flag
            conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
            cursor = conn.cursor()
            if item["Listing_Date"] != "-":
                update_sql_company = "update company set ipo_date=%s,gmt_update=%s,user_update=%s where code=%s"
                cursor.execute(update_sql_company, [item["Listing_Date"], item["gmt_create"], "zx", item["code"]])
                conn.commit()

            for each in item:
                if "_HCK" in each:
                    name = str(each).replace("_HCK", "")
                    sql_select = "select id from company_profile_definition where name = %s"
                    cursor.execute(sql_select, each)
                    result = cursor.fetchone()
                    company_profile_definition_id = result[0]
                    parameter_detail = [
                        item[name],
                        item["gmt_create"],
                        "zx",
                        item["code"],
                        company_profile_definition_id
                    ]
                    sql_jud = "select value from company_profile_detail where company_code = %s and company_profile_definition_id = %s"
                    cursor.execute(sql_jud, [item["code"], company_profile_definition_id])
                    jud_result = cursor.fetchone()
                    if jud_result is not None:
                        if str(item[name]) != str(jud_result[0]):
                            sql_update = "update company_profile_detail set value = %s, gmt_update=%s, user_create=%s where company_code = %s and company_profile_definition_id = %s"
                            cursor.execute(sql_update, parameter_detail)
                            conn.commit()
                    else:
                        parameter_d = [
                            company_profile_definition_id,
                            item["code"],
                            item[name],
                            item["gmt_create"],
                            "zx"
                        ]
                        sql_insert = "insert into company_profile_detail(company_profile_definition_id,company_code,value,gmt_create,user_create)values(%s,%s,%s,%s,%s)"
                        cursor.execute(sql_insert, parameter_d)
                        conn.commit()
            conn.close()
            cursor.close()
        return item


class ChinaChangeNamePipeline(object):
    def process_item(self, item, spider):
        if spider.name == "HCK_infor_for_web":
            conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA",
                                   charset="utf8")
            cursor = conn.cursor()
            params = [
                item["website_url"],
                item["code"]
            ]
            sql = "update company set website_url=%s where security_code=%s"
            cursor.execute(sql, params)
            conn.commit()
            conn.close()
            cursor.close()
        return item
