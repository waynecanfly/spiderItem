# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


flag = 0


class ChinaDetailPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "HCK_information":
            global flag
            update_sql_company = "update company set ipo_date=%s,gmt_update=%s,user_update=%s where code=%s"
            self.cursor.execute(update_sql_company, [item["Listing_Date"], item["gmt_create"], "zx", item["code"]])
            self.conn.commit()
            update_sql_dataSource = "update company_data_source set mark=%s,gmt_update=%s,user_update=%s where company_id=%s"
            self.cursor.execute(update_sql_dataSource, [1, item["gmt_create"], "zx", item["code"]])
            self.conn.commit()
            if flag == 0:
                flag += 1
                for temp in item:
                    if "_HCK" in temp:
                        display_label = item[temp]
                        data_type = "string"
                        sort = 0
                        parameter = [
                            temp,
                            display_label,
                            data_type,
                            sort,
                            item["gmt_create"],
                            "zx"
                        ]
                        sql_jud = "select id from company_profile_definition where name = %s"
                        self.cursor.execute(sql_jud, temp)
                        results = self.cursor.fetchall()
                        if len(results) == 0:
                            sql = "insert into company_profile_definition(name,display_label,data_type,sort,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s)"
                            self.cursor.execute(sql, parameter)
                            self.conn.commit()

            for each in item:
                if "_HCK" in each:
                    jud_list = []
                    name = str(each).replace("_HCK", "")
                    sql_select = "select id from company_profile_definition where name = %s"
                    self.cursor.execute(sql_select, each)
                    result = self.cursor.fetchone()
                    company_profile_definition_id = result[0]
                    parameter_detail = [
                        company_profile_definition_id,
                        item["code"],
                        str(item[name]),
                        item["gmt_create"],
                        "zx"
                    ]
                    sql_jud_detail = "select company_profile_definition_id from company_profile_detail where company_code = %s"
                    self.cursor.execute(sql_jud_detail, item["code"])
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


class ChinaChangeNamePipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "HCK_infor_for_web":
            params = [
                item["website_url"],
                item["code"]
            ]
            sql = "update company set website_url=%s where security_code=%s"
            self.cursor.execute(sql, params)
            self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()


class SZSENewAddPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "HCK_pdf_spider":
            if item["financial_statement_season_type_code"] != "":
                params = [
                    item["disclosure_date"],
                    item["file_name"],
                    item["doc_source_url"],
                    item["exchange_market_code"],
                    item["company_code"],
                    item["financial_statement_season_type_code"],
                    item["announcement_type"],
                    item["fiscal_year"],
                    item["report_id"],
                    item["doc_local_path"],
                    item["country_code"],
                    item["is_doc_url_direct"],
                    item["financial_reporting_standard_code"],
                    item["doc_type"],
                    item["is_downloaded"],
                    item["currency_code"],
                    item["language_written_code"],
                    item["doc_downloaded_timestamp"],
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql = "insert into financial_statement_index(disclosure_date,file_original_title,doc_source_url," \
                      "exchange_market_code,company_code,financial_statement_season_type_code,announcement_type," \
                      "fiscal_year,report_id,doc_local_path,country_code,is_doc_url_direct,financial_reporting_standard_code," \
                      "doc_type,is_downloaded,currency_code,language_written_code,doc_downloaded_timestamp,gmt_create," \
                      "user_create)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()

            else:
                params = [
                    item["disclosure_date"],
                    item["file_name"],
                    item["doc_source_url"],
                    item["exchange_market_code"],
                    item["company_code"],
                    item["detail_type"],
                    item["fiscal_year"],
                    item["report_id"],
                    item["doc_local_path"],
                    item["country_code"],
                    item["is_doc_url_direct"],
                    item["doc_type"],
                    item["is_downloaded"],
                    item["language_written_code"],
                    item["doc_downloaded_timestamp"],
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql = "insert into non_financial_statement_index(disclosure_date,file_original_title,doc_source_url," \
                      "exchange_market_code,company_code,Non_financial_announcement_detail_type," \
                      "fiscal_year,report_id,doc_local_path,country_code,is_doc_url_direct," \
                      "doc_type,is_downloaded,language_written_code,doc_downloaded_timestamp,gmt_create," \
                      "user_create)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()

        # if spider.name == "HCK_pdf_for_loss":
        #     params = [
        #         item["is_downloaded"],
        #         item["gmt_update"],
        #         item["doc_downloaded_timestamp"],
        #         item["user_update"],
        #         item["report_id"]
        #     ]
        #     sql = "update HCK_pdf set is_downloaded=%s,gmt_update=%s,doc_downloaded_timestamp=%s,user_update=%s where report_id=%s"
        #     self.cursor.execute(sql, params)
        #     self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()