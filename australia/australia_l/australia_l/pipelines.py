# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


class AustraliacompanyupdatePipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "DownloadPdf-ASX" or spider.name == "DownloadPdf-NSX":
            if item["announcement_type"] == 1:
                params = [
                    item["country_code"],
                    item["exchange_market_code"],
                    item["company_code"],
                    item["financial_statement_season_type_code"],
                    item["financial_reporting_standard_code"],
                    item["disclosure_date"],
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
                    item["original_title"]
                ]
                sql = "insert into financial_statement_index(country_code,exchange_market_code,company_code," \
                      "financial_statement_season_type_code,financial_reporting_standard_code,disclosure_date," \
                      "language_written_code,doc_type,doc_source_url,is_doc_url_direct,doc_local_path," \
                      "doc_downloaded_timestamp,is_downloaded,currency_code,gmt_create,user_create,fiscal_year," \
                      "report_id,file_original_title)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()
            else:
                params = [
                    item["country_code"],
                    item["exchange_market_code"],
                    item["company_code"],
                    item["fiscal_year"],
                    item["disclosure_date"],
                    item["doc_type"],
                    item["doc_source_url"],
                    item["is_doc_url_direct"],
                    item["doc_local_path"],
                    item["doc_downloaded_timestamp"],
                    item["is_downloaded"],
                    item["gmt_create"],
                    item["user_create"],
                    item["report_id"],
                    item["language_written_code"],
                    item["original_title"]
                ]
                sql = "insert into non_financial_statement_index(country_code,exchange_market_code,company_code,fiscal_year," \
                      "disclosure_date,doc_type,doc_source_url,is_doc_url_direct,doc_local_path,doc_downloaded_timestamp," \
                      "is_downloaded,gmt_create,user_create,report_id,language_written_code,file_original_title)" \
                      "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()


class AustraliacompanyupdateASX(object):
    flag = 0

    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "BasicInfo_ASX":
            spider_name = "BasicInfo_ASX_headlessChrome"
            is_batch = 1
            download_link = "https://www.asx.com.au/asx/research/listedCompanies.do"
            parameter_data_source = [
                item["Title"],
                download_link,
                spider_name,
                is_batch,
                item["gmt_create"],
                "zx",
                1,
                item["company_id"]
            ]
            sql_data_source = "update company_data_source set company_name=%s, download_link=%s, spider_name=%s, is_batch=%s, gmt_create=%s, user_create=%s, mark=%s where company_id = %s"
            self.cursor.execute(sql_data_source, parameter_data_source)
            self.conn.commit()
            sql_jud = "select id from company where code = %s"
            self.cursor.execute(sql_jud, item["company_id"])
            results = self.cursor.fetchall()
            if len(results) == 0:
                parameter_company = [
                    item["company_id"],
                    item["Title"],
                    item["Title"],
                    item["Security_code"],
                    "AUS",
                    "ASX",
                    item["Official_listing_date"],
                    "AUD",
                    item["Internet_address"],
                    item["gmt_create"],
                    "zx"
                ]
                sql_company = "insert into company(code,name_origin,name_en,security_code,country_code_listed,exchange_market_code,ipo_date,currency_code,website_url,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql_company, parameter_company)
                self.conn.commit()
            if self.flag == 0:
                self.flag += 1
                for temp in item:
                    if "_title" in temp:
                        name = str(temp).replace("_title", "")
                        display_label = name
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
                    jud_list = []
                    name = str(each).replace("_title", "")
                    sql_select = "select id from company_profile_definition where name = %s"
                    self.cursor.execute(sql_select, name)
                    result = self.cursor.fetchone()
                    company_profile_definition_id = result[0]
                    parameter_detail = [
                        company_profile_definition_id,
                        item["company_id"],
                        item[each],
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