# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


flag = 0


class IndiaPipelineExcel(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "downloadExcel_BSE":
            params_india = [
                item["country_code"],
                item["exchange_market_code"],
                item["company_code"],
                item["fiscal_year"],
                item["financial_statement_type_code"],
                item["financial_reporting_standard_code"],
                item["start_date"],
                item["end_date"],
                item["language_written_code"],
                item["doc_type"],
                item["source_url"],
                item["is_doc_url_direct"],
                item["doc_local_path"],
                item["doc_downloaded_timestamp"],
                item["is_downloaded"],
                item["currency_code"],
                item["gmt_create"],
                item["user_create"],
                item["report_id"],
                item["financial_statement_season_type_code"]
            ]
            sql = "insert into financial_statement_index(country_code,exchange_market_code,company_code,fiscal_year," \
                  "financial_statement_type_code,financial_reporting_standard_code,start_date,end_date," \
                  "language_written_code,doc_type,doc_source_url,is_doc_url_direct,doc_local_path," \
                  "doc_downloaded_timestamp,is_downloaded,currency_code,gmt_create,user_create,report_id," \
                  "financial_statement_season_type_code)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql, params_india)
            self.conn.commit()

        if spider.name == "downloadExcel_NSE":
            params = [
                item["fiscal_year"],
                item["source_url"],
                item["company_code"],
                item["country_code"],
                item["exchange_market_code"],
                item["financial_reporting_standard_code"],
                item["doc_type"],
                item["is_doc_url_direct"],
                item["is_downloaded"],
                item["currency_code"],
                item["doc_downloaded_timestamp"],
                item["language_written_code"],
                item["report_id"],
                item["doc_local_path"],
                item["gmt_create"],
                item["user_create"],
                item["financial_statement_season_type_code"],
                item["end_date"],
                item["start_date"]
            ]
            sql = "insert into financial_statement_index(fiscal_year,doc_source_url,company_code,country_code," \
                  "exchange_market_code,financial_reporting_standard_code,doc_type,is_doc_url_direct,is_downloaded," \
                  "currency_code,doc_downloaded_timestamp,language_written_code,report_id,doc_local_path,gmt_create," \
                  "user_create,financial_statement_season_type_code,end_date,start_date)values(%s,%s,%s,%s,%s,%s,%s," \
                  "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql, params)
            self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()


class IndiaPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "downloadPdf_BSE_A":
            params = [
                item["fiscal_year"],
                item["doc_source_url"],
                item["company_code"],
                item["country_code"],
                item["exchange_market_code"],
                item["financial_reporting_standard_code"],
                item["doc_type"],
                item["is_doc_url_direct"],
                item["is_downloaded"],
                item["currency_code"],
                item["doc_downloaded_timestamp"],
                item["language_written_code"],
                item["report_id"],
                item["doc_local_path"],
                item["gmt_create"],
                item["user_create"],
                item["financial_statement_season_type_code"]
            ]
            sql = "insert into financial_statement_index(fiscal_year,doc_source_url,company_code,country_code," \
                  "exchange_market_code,financial_reporting_standard_code,doc_type,is_doc_url_direct,is_downloaded," \
                  "currency_code,doc_downloaded_timestamp,language_written_code,report_id,doc_local_path,gmt_create," \
                  "user_create,financial_statement_season_type_code)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql, params)
            self.conn.commit()

        if spider.name == "downloadPdf_BSE_Q":
            if item["announcement"] == 1:
                params = [
                    item["fiscal_year"],
                    item["doc_source_url"],
                    item["company_code"],
                    item["country_code"],
                    item["exchange_market_code"],
                    item["financial_reporting_standard_code"],
                    item["doc_type"],
                    item["is_doc_url_direct"],
                    item["is_downloaded"],
                    item["currency_code"],
                    item["doc_downloaded_timestamp"],
                    item["language_written_code"],
                    item["report_id"],
                    item["doc_local_path"],
                    item["gmt_create"],
                    item["user_create"],
                    item["financial_statement_season_type_code"],
                    item["disclosure_date"],
                    item["end_date"],
                    item["pdf_name"]
                ]
                sql = "insert into financial_statement_index(fiscal_year,doc_source_url,company_code,country_code," \
                      "exchange_market_code,financial_reporting_standard_code,doc_type,is_doc_url_direct,is_downloaded," \
                      "currency_code,doc_downloaded_timestamp,language_written_code,report_id,doc_local_path,gmt_create," \
                      "user_create,financial_statement_season_type_code,disclosure_date,end_date,file_original_title)" \
                      "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()
            else:
                params = [
                    item["fiscal_year"],
                    item["doc_source_url"],
                    item["company_code"],
                    item["country_code"],
                    item["exchange_market_code"],
                    item["doc_type"],
                    item["is_doc_url_direct"],
                    item["is_downloaded"],
                    item["doc_downloaded_timestamp"],
                    item["language_written_code"],
                    item["report_id"],
                    item["doc_local_path"],
                    item["gmt_create"],
                    item["user_create"],
                    item["disclosure_date"],
                    item["pdf_name"]
                ]
                sql = "insert into non_financial_statement_index(fiscal_year,doc_source_url,company_code,country_code," \
                      "exchange_market_code,doc_type,is_doc_url_direct,is_downloaded," \
                      "doc_downloaded_timestamp,language_written_code,report_id,doc_local_path,gmt_create," \
                      "user_create,disclosure_date,file_original_title)" \
                      "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()

        if spider.name == "downloadZip_NSE":
            params = [
                item["fiscal_year"],
                item["doc_source_url"],
                item["company_code"],
                item["country_code"],
                item["exchange_market_code"],
                item["financial_reporting_standard_code"],
                item["doc_type"],
                item["is_doc_url_direct"],
                item["is_downloaded"],
                item["currency_code"],
                item["doc_downloaded_timestamp"],
                item["language_written_code"],
                item["report_id"],
                item["doc_local_path"],
                item["gmt_create"],
                item["user_create"],
                item["financial_statement_season_type_code"],
                item["end_date"],
                item["start_date"]
            ]
            sql = "insert into financial_statement_index(fiscal_year,doc_source_url,company_code,country_code," \
                  "exchange_market_code,financial_reporting_standard_code,doc_type,is_doc_url_direct,is_downloaded," \
                  "currency_code,doc_downloaded_timestamp,language_written_code,report_id,doc_local_path,gmt_create," \
                  "user_create,financial_statement_season_type_code,end_date,start_date)values(%s,%s,%s,%s,%s,%s,%s," \
                  "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql, params)
            self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()
