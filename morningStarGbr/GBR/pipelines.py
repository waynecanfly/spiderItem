# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from GBR.items import GbrItem, GbrItem2


class GbrPipeline(object):
    def __init__(self):
        # self.conn = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root", passwd="OPDATA", charset="utf8")
        # self.cursor = self.conn.cursor()
        self.conn2 = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor2 = self.conn2.cursor()

    def process_item(self, item, spider):
        # if spider.name == "for_china_loss":
        #     params = [
        #         item["is_downloaded"],
        #         item["gmt_update"],
        #         item["doc_downloaded_timestamp"],
        #         item["user_update"],
        #         0,
        #         item["report_id"]
        #     ]
        #     sql = "update financial_statement_index set is_downloaded=%s,gmt_update=%s,doc_downloaded_timestamp=%s,user_update=%s,pdf_state=%s where report_id=%s"
        #     self.cursor2.execute(sql, params)
        #     self.conn2.commit()
        # if spider.name == "for_loss":
        #     params333 = [
        #         item["is_downloaded"],
        #         item["gmt_update"],
        #         item["doc_downloaded_timestamp"],
        #         item["user_update"],
        #         item["report_id"]
        #     ]
        #     sql = "update GBR set is_downloaded=%s,gmt_update=%s,doc_downloaded_timestamp=%s,user_update=%s where report_id=%s"
        #     self.cursor.execute(sql, params333)
        #     self.conn.commit()
        if isinstance(item, GbrItem):
            if item["detail_type"] == "":
                params = [
                    item["country_code"],
                    item["exchange_market_code"],
                    item["company_code"],
                    item["fiscal_year"],
                    item["financial_statement_season_type_code"],
                    item["disclosure_date"],
                    item["language_written_code"],
                    item["doc_type"],
                    item["doc_source_url"],
                    item["is_doc_url_direct"],
                    item["doc_local_path"],
                    item["doc_downloaded_timestamp"],
                    item["is_downloaded"],
                    item["gmt_create"],
                    item["user_create"],
                    item["report_id"],
                    item["pdf_name"]
                ]
                sql = "insert into financial_statement_index(country_code,exchange_market_code," \
                      "company_code,fiscal_year,financial_statement_season_type_code," \
                      "disclosure_date,language_written_code,doc_type,doc_source_url,is_doc_url_direct," \
                      "doc_local_path,doc_downloaded_timestamp,is_downloaded,gmt_create,user_create,report_id," \
                      "file_original_title)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor2.execute(sql, params)
                self.conn2.commit()
            else:
                params = [
                    item["country_code"],
                    item["exchange_market_code"],
                    item["company_code"],
                    item["fiscal_year"],
                    item["detail_type"],
                    item["disclosure_date"],
                    item["language_written_code"],
                    item["doc_type"],
                    item["doc_source_url"],
                    item["is_doc_url_direct"],
                    item["doc_local_path"],
                    item["doc_downloaded_timestamp"],
                    item["is_downloaded"],
                    item["gmt_create"],
                    item["user_create"],
                    item["report_id"],
                    item["pdf_name"]
                ]
                sql = "insert into non_financial_statement_index(country_code,exchange_market_code," \
                      "company_code,fiscal_year,Non_financial_announcement_detail_type," \
                      "disclosure_date,language_written_code,doc_type,doc_source_url,is_doc_url_direct," \
                      "doc_local_path,doc_downloaded_timestamp,is_downloaded,gmt_create,user_create,report_id," \
                      "file_original_title)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor2.execute(sql, params)
                self.conn2.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        # self.cursor.close()
        # self.conn.close()
        self.cursor2.close()
        self.conn2.close()
