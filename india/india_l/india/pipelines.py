# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


flag = 0
exclude_list = ["company_id","name_origin","name_en","security_code","country_code_listed","exchange_market_code","ipo_date","currency_code","status","website_url","gmt_create","user_create","info_disclosure_id","ISIN"]


class IndiaPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "BasicInfoBSE" or spider.name == "BasicInfoNSE":
            is_batch = 1
            parameter_data_source = [
                is_batch,
                item["gmt_create"],
                item["user_create"],
                1,
                item["company_id"]
            ]
            sql_data_source = "update company_data_source set is_batch=%s, gmt_create=%s, user_create=%s, mark=%s where company_id = %s"
            self.cursor.execute(sql_data_source, parameter_data_source)
            self.conn.commit()
            sql_jud = "select id from company where code = %s"
            self.cursor.execute(sql_jud, item["company_id"])
            results = self.cursor.fetchall()
            if len(results) == 0:
                parameter_company = [
                    item["company_id"],
                    item["name_origin"],
                    item["name_en"],
                    item["security_code"],
                    item["country_code_listed"],
                    item["exchange_market_code"],
                    item["ipo_date"],
                    item["currency_code"],
                    item["status"],
                    item["website_url"],
                    item["gmt_create"],
                    item["user_create"],
                    item["info_disclosure_id"],
                    item["ISIN"]
                ]
                sql_company = "insert into company(code,name_origin,name_en,security_code,country_code_listed,exchange_market_code,ipo_date,currency_code,status,website_url,gmt_create,user_create,info_disclosure_id,isin)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql_company, parameter_company)
                self.conn.commit()

            global flag
            if flag == 0:
                flag += 1
                for temp in item:
                    if temp not in exclude_list:
                        name = temp + "_IND"
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
                if each not in exclude_list:
                    jud_list = []
                    name = each
                    sql_select = "select id from company_profile_definition where name = %s"
                    self.cursor.execute(sql_select, name + "_IND")
                    result = self.cursor.fetchone()
                    company_profile_definition_id = result[0]
                    parameter_detail = [
                        company_profile_definition_id,
                        item["company_id"],
                        item[name],
                        item["gmt_create"],
                        "zx"
                    ]
                    sql_jud_detail = "select company_profile_definition_id from company_profile_detail where company_code = %s"
                    self.cursor.execute(sql_jud_detail, item["company_id"])
                    results_jud = self.cursor.fetchall()
                    for each_id in results_jud:
                        if each_id not in jud_list:
                            jud_list.append(each_id[0])
                    if company_profile_definition_id not in jud_list:
                        sql_insert = "insert into company_profile_detail(company_profile_definition_id,company_code,value,gmt_create,user_create)values(%s,%s,%s,%s,%s)"
                        self.cursor.execute(sql_insert, parameter_detail)
                        self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()
        # if spider.name == "downloadExcel_BSE":
        #     conn = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root", passwd="originp", charset="utf8")
        #     cursor = conn.cursor()
        #     params_india = [
        #         item["country_code"],
        #         item["exchange_market_code"],
        #         item["company_code"],
        #         item["fiscal_year"],
        #         item["financial_statement_type_code"],
        #         item["financial_reporting_standard_code"],
        #         item["start_date"],
        #         item["end_date"],
        #         item["language_written_code"],
        #         item["doc_type"],
        #         item["source_url"],
        #         item["is_doc_url_direct"],
        #         item["doc_local_path"],
        #         item["doc_downloaded_timestamp"],
        #         item["is_downloaded"],
        #         item["currency_code"],
        #         item["gmt_create"],
        #         item["user_create"],
        #         item["report_id"]
        #     ]
        #
        #     sql = "insert into financial_statement_index_indiaUpdate_20180508(country_code,exchange_market_code,company_code,fiscal_year,financial_statement_type_code,financial_reporting_standard_code,start_date,end_date,language_written_code,doc_type,doc_source_url,is_doc_url_direct,doc_local_path,doc_downloaded_timestamp,is_downloaded,currency_code,gmt_create,user_create,report_id)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #     cursor.execute(sql, params_india)
        #     conn.commit()
        #     conn.close()
        #     cursor.close()
        #
        # if spider.name == "downloadPdf_BSE_A":
        #     conn = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root", passwd="originp", charset="utf8")
        #     cursor = conn.cursor()
        #     params = [
        #         item["fiscal_year"],
        #         item["doc_source_url"],
        #         item["company_code"],
        #         item["country_code"],
        #         item["exchange_market_code"],
        #         item["financial_reporting_standard_code"],
        #         item["doc_type"],
        #         item["is_doc_url_direct"],
        #         item["is_downloaded"],
        #         item["currency_code"],
        #         item["doc_downloaded_timestamp"],
        #         item["language_written_code"],
        #         item["report_id"],
        #         item["doc_local_path"],
        #         item["gmt_create"],
        #         item["user_create"],
        #         item["financial_statement_season_type_code"]
        #     ]
        #     sql = "insert into financial_statement_index_indiaUpdate_20180508(fiscal_year,doc_source_url,company_code,country_code,exchange_market_code,financial_reporting_standard_code,doc_type,is_doc_url_direct,is_downloaded,currency_code,doc_downloaded_timestamp,language_written_code,report_id,doc_local_path,gmt_create,user_create,financial_statement_season_type_code)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #     cursor.execute(sql, params)
        #     conn.commit()
        #     conn.close()
        #     cursor.close()
        #
        # if spider.name == "downloadPdf_BSE_Q":
        #     conn = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root", passwd="originp", charset="utf8")
        #     cursor = conn.cursor()
        #     params = [
        #         item["fiscal_year"],
        #         item["doc_source_url"],
        #         item["company_code"],
        #         item["country_code"],
        #         item["exchange_market_code"],
        #         item["financial_reporting_standard_code"],
        #         item["doc_type"],
        #         item["is_doc_url_direct"],
        #         item["is_downloaded"],
        #         item["currency_code"],
        #         item["doc_downloaded_timestamp"],
        #         item["language_written_code"],
        #         item["report_id"],
        #         item["doc_local_path"],
        #         item["gmt_create"],
        #         item["user_create"],
        #         item["financial_statement_season_type_code"],
        #         item["disclosure_date"],
        #         item["end_date"]
        #     ]
        #     sql = "insert into financial_statement_index_indiaUpdate_20180508(fiscal_year,doc_source_url,company_code,country_code,exchange_market_code,financial_reporting_standard_code,doc_type,is_doc_url_direct,is_downloaded,currency_code,doc_downloaded_timestamp,language_written_code,report_id,doc_local_path,gmt_create,user_create,financial_statement_season_type_code,disclosure_date,end_date)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #     cursor.execute(sql, params)
        #     conn.commit()
        #     conn.close()
        #     cursor.close()
        # if spider.name == "downloadExcel_NSE":
        #     conn = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root",passwd="originp", charset="utf8")
        #     cursor = conn.cursor()
        #     params = [
        #         item["fiscal_year"],
        #         item["source_url"],
        #         item["company_code"],
        #         item["country_code"],
        #         item["exchange_market_code"],
        #         item["financial_reporting_standard_code"],
        #         item["doc_type"],
        #         item["is_doc_url_direct"],
        #         item["is_downloaded"],
        #         item["currency_code"],
        #         item["doc_downloaded_timestamp"],
        #         item["language_written_code"],
        #         item["report_id"],
        #         item["doc_local_path"],
        #         item["gmt_create"],
        #         item["user_create"],
        #         item["financial_statement_season_type_code"],
        #         item["end_date"],
        #         item["start_date"]
        #     ]
        #     sql = "insert into financial_statement_index_indiaUpdate_20180508(fiscal_year,doc_source_url,company_code,country_code,exchange_market_code,financial_reporting_standard_code,doc_type,is_doc_url_direct,is_downloaded,currency_code,doc_downloaded_timestamp,language_written_code,report_id,doc_local_path,gmt_create,user_create,financial_statement_season_type_code,end_date,start_date)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #     cursor.execute(sql, params)
        #     conn.commit()
        #     conn.close()
        #     cursor.close()
        # if spider.name == "downloadZip_NSE":
        #     conn = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root",passwd="originp", charset="utf8")
        #     cursor = conn.cursor()
        #     params = [
        #         item["fiscal_year"],
        #         item["doc_source_url"],
        #         item["company_code"],
        #         item["country_code"],
        #         item["exchange_market_code"],
        #         item["financial_reporting_standard_code"],
        #         item["doc_type"],
        #         item["is_doc_url_direct"],
        #         item["is_downloaded"],
        #         item["currency_code"],
        #         item["doc_downloaded_timestamp"],
        #         item["language_written_code"],
        #         item["report_id"],
        #         item["doc_local_path"],
        #         item["gmt_create"],
        #         item["user_create"],
        #         item["financial_statement_season_type_code"],
        #         item["end_date"],
        #         item["start_date"]
        #     ]
        #     sql = "insert into financial_statement_index_indiaUpdate_20180508(fiscal_year,doc_source_url,company_code,country_code,exchange_market_code,financial_reporting_standard_code,doc_type,is_doc_url_direct,is_downloaded,currency_code,doc_downloaded_timestamp,language_written_code,report_id,doc_local_path,gmt_create,user_create,financial_statement_season_type_code,end_date,start_date)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #     cursor.execute(sql, params)
        #     conn.commit()
        #     conn.close()
        #     cursor.close()
