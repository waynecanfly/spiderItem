# -*- coding: utf-8 -*-

# Define your item pipelines here
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import pymysql


class ChinaReportPipeline(object):
    def process_item(self, item, spider):
        conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        cursor = conn.cursor()

        params = [
            item["country_code"],
            item["exchange_market_code"],
            item["company_code"],
            item["fiscal_year"],
            item["financial_statement_season_type_code"],
            item["financial_reporting_standard_code"],
            item["disclosure_date"],
            item["file_name"],
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
            item["report_id"]
        ]
        sql = "insert into financial_statement_index(country_code,exchange_market_code,company_code,fiscal_year,financial_statement_season_type_code,financial_reporting_standard_code,disclosure_date,file_original_title,language_written_code,doc_type,doc_source_url,is_doc_url_direct,doc_local_path,doc_downloaded_timestamp,is_downloaded,currency_code,gmt_create,user_create,report_id)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, params)
        conn.commit()
        conn.close()
        cursor.close()
        return item