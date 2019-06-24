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
        if spider.name == "FrankfurtCompanyList":
            if self.flag == 0:
                self.flag += 1
                self.create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                self.create_time = str(self.create_time).replace("-", "").replace(" ", "").replace(":", "")
            with open("/data/OPDCMS/germany/listed_company_update/company_list/frankfurt" + "_" + self.create_time + ".csv","a") as f:
                f.write(item["Name"] + "," + item["wkn"] + "," + item["isin"] + "\n")

        if spider.name == "FrankfurtBasicInfo":
            conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
            cursor = conn.cursor()
            spider_name = "FrankfurtBasicInfo"
            is_batch = 1
            download_link = "http://en.boerse-frankfurt.de/search/advancedsharesearchresults?CountryID=1&IsPreferredStock=false&view=financial&p=1&count=25"
            parameter_data_source = [
                item["name_origin"],
                download_link,
                spider_name,
                is_batch,
                item["gmt_create"],
                item["user_create"],
                1,
                item["company_id"]
            ]
            sql_data_source = "update company_data_source set company_name=%s, download_link=%s, spider_name=%s, is_batch=%s, gmt_create=%s, user_create=%s, mark=%s where company_id = %s"
            cursor.execute(sql_data_source, parameter_data_source)
            conn.commit()
            sql_jud = "select id from company where code = %s"
            cursor.execute(sql_jud, item["company_id"])
            results = cursor.fetchall()
            if len(results) == 0:
                parameter_company = [
                    item["company_id"],
                    item["name_origin"],
                    item["country_code_listed"],
                    item["exchange_market_code"],
                    item["currency_code"],
                    item["Web"],
                    item["gmt_create"],
                    item["user_create"],
                    item["info_disclosure_id"],
                    item["Sector"]
                ]
                sql_company = "insert into company(code,name_origin,country_code_listed,exchange_market_code,currency_code,website_url,gmt_create,user_create,info_disclosure_id,industry)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql_company, parameter_company)
                conn.commit()

            if self.flag2 == 0:
                self.flag2 += 1
                for temp in item:
                    if temp not in self.exclusion_list:
                        name = temp
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
                        cursor.execute(sql_jud, name)
                        results = cursor.fetchall()
                        if len(results) == 0:
                            sql = "insert into company_profile_definition(name,display_label,data_type,sort,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s)"
                            cursor.execute(sql, parameter)
                            conn.commit()

            for each in item:
                if each not in self.exclusion_list:
                    jud_list = []
                    sql_select = "select id from company_profile_definition where name = %s"
                    cursor.execute(sql_select, each)
                    result = cursor.fetchone()
                    company_profile_definition_id = result[0]
                    parameter_detail = [
                        company_profile_definition_id,
                        item["company_id"],
                        item[each],
                        item["gmt_create"],
                        "zx"
                    ]
                    sql_jud_detail = "select company_profile_definition_id from company_profile_detail where company_code = %s"
                    cursor.execute(sql_jud_detail, item["company_id"])
                    results_jud = cursor.fetchall()
                    for each_id in results_jud:
                        if each_id not in jud_list:
                            jud_list.append(each_id[0])
                    if company_profile_definition_id not in jud_list:
                        sql_insert = "insert into company_profile_detail(company_profile_definition_id,company_code,value,gmt_create,user_create)values(%s,%s,%s,%s,%s)"
                        cursor.execute(sql_insert, parameter_detail)
                        conn.commit()

        if spider.name == "Frankfurtpdf":
            conn = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root",passwd="originp", charset="utf8")
            cursor = conn.cursor()
            params = [
                item["country_code"],
                item["exchange_market_code"],
                item["company_code"],
                item["financial_statement_season_type_code"],
                item["financial_reporting_standard_code"],
                item["language_written_code"],
                item["doc_type"],
                item["doc_source_url"],
                item["is_doc_url_direct"],
                item["doc_local_path"],
                item["doc_downloaded_timestamp"],
                item["is_downloaded"],
                item["currency_code"],
                item["gmt_create"],
                item["user_create"],
                item["fiscal_year"],
                item["report_id"],
                item["origin_pdf_name"],
                item["start_date"],
                item["end_date"]
            ]
            sql = "insert into financial_statement_index_germanyUpdate_20180426(country_code,exchange_market_code,company_code,financial_statement_season_type_code,financial_reporting_standard_code,language_written_code,doc_type,doc_source_url,is_doc_url_direct,doc_local_path,doc_downloaded_timestamp,is_downloaded,currency_code,gmt_create,user_create,fiscal_year,report_id, pdf_name, start_date, end_date)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, params)
            conn.commit()
            conn.close()
            cursor.close()
        return item
