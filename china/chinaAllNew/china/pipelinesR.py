# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
"""company表和detail表都有做重复判断"""
import json
import pymysql
import time
from china.items import ChinaIntroItem_ad
from china.items import ChinaIntroItem_cs
from china.items import ChinaIntroItem_dsn
from china.items import ChinaIntroItem_if
from china.items import ChinaIntroItem_sbi
from china.items import ChinaIntroItem_si
from china.items import ChinaIntroItem_stp
from china.items import ChinaIntroItem_str
from china.items import ChinaIntroItem_xsg
from china.items import ChinaIntroItem_dd
from china.items import ChinaIntroItem_shenzhen_status, ChinaIntroItem_shenzhen_nameChange
from china.items import ChinaIntroItem_sh, ChinaIntroItem_sh_non

flag = 0
flag2 = 0
flag3 = 0
flag4 = 0
flag5 = 0
flag6 = 0
create_time = None


class ShenInfoPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "china_updateAll_r":
            global flag, flag2, flag3, flag4, flag5, flag6
            # if spider.name == "shenzhen_sensible_information":
            if isinstance(item, ChinaIntroItem_sbi):
                if flag == 0:
                    flag += 1
                    sql_de = "delete from shenzhen_sensible_information"
                    self.cursor.execute(sql_de)
                params = [
                    item["exchange_market_code"],
                    item["security_code"],
                    item["disclosure_date"],
                    item["file_name"],
                    item["doc_type"],
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql = "insert into shenzhen_sensible_information(exchange_market_code,security_code,publish_time,sensible_name,sensible_type,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()
            # if spider.name == "shenzhen_sensible_talent_pool":
            if isinstance(item, ChinaIntroItem_stp):
                if flag2 == 0:
                    flag2 += 1
                    sql_de = "delete from shenzhen_sensible_talent_pool"
                    self.cursor.execute(sql_de)
                params = [
                    item["college_name"],
                    item["exchange_market_code"],
                    item["training_date"],
                    item["training_num"],
                    item["gender"],
                    item["age"],
                    item["education"],
                    item["discipline"],
                    item["job_title"],
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql = "insert into shenzhen_sensible_talent_pool(college_name, exchange_market_code, training_date, training_num, gender, age, education, discipline, job_title,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()

            # if spider.name == "shenzhen_secretary_information":
            if isinstance(item, ChinaIntroItem_si):
                if flag3 == 0:
                    flag3 += 1
                    sql_de = "delete from shenzhen_secretary_information"
                    self.cursor.execute(sql_de)
                params = [
                    item["security_code"],
                    item["secretary_name"],
                    item["exchange_market_code"],
                    item["start_date"],
                    item["recent_training_date"],
                    item["gender"],
                    item["job_title"],
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql = "insert into shenzhen_secretary_information(security_code,secretary_name,exchange_market_code,start_date,recent_training_date,gender,job_title,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()

            # if spider.name == "shenzhen_secretary_training_record":
            if isinstance(item, ChinaIntroItem_str):
                if flag4 == 0:
                    flag4 += 1
                    sql_de = "delete from shenzhen_secretary_training_record"
                    self.cursor.execute(sql_de)
                params = [
                    item["secretary_name"],
                    item["exchange_market_code"],
                    item["start_date"],
                    item["recent_training_date"],
                    item["gender"],
                    item["education"],
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql = "insert into shenzhen_secretary_training_record(secretary_name,exchange_market_code,obtain_qualification_date,recent_training_date,gender,education,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()
            # if spider.name == "shenzhen_xsgfjxyjc":
            if isinstance(item, ChinaIntroItem_xsg):
                if flag5 == 0:
                    flag5 += 1
                    sql_de = "delete from shenzhen_xsgfjxyjc"
                    self.cursor.execute(sql_de)
                params = [
                    item["security_code"],
                    item["release_sale_date"],
                    item["release_sale_shareholder_num"],
                    item["release_sale_stock_num"],
                    item["release_sale_stock_rate"],
                    item["exchange_market_code"],
                    item["shareholder_name"],
                    item["managed_member_name"],
                    item["disclosure_date"],
                    item["bcpljcgf_num"],
                    item["bcpljcgf_rate"],
                    item["tgdzjyjc_num"],
                    item["tgdzjyjc_rate"],
                    item["jchrcygf_num"],
                    item["jchrcygf_rate"],
                    item["belong_2_type"],
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql = "insert into shenzhen_xsgfjxyjc(security_code,release_sale_date,release_sale_shareholder_num,release_sale_stock_num,release_sale_stock_rate,exchange_market_code,shareholder_name,managed_member_name,disclosure_date,bcpljcgf_num,bcpljcgf_rate,tgdzjyjc_num,tgdzjyjc_rate,jchrcygf_num,jchrcygf_rate,belong_2_type,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()
            #nameChange
            if isinstance(item, ChinaIntroItem_shenzhen_nameChange):
                sql_select = "select code from company where security_code=%s"
                self.cursor.execute(sql_select, item["code"])
                result = self.cursor.fetchone()
                if result:
                    sql_jud = "select id from company_former_name where company_code=%s and name=%s"
                    self.cursor.execute(sql_jud, [result[0], item["fomer_name"]])
                    result_jud = self.cursor.fetchall()
                    if len(result_jud) == 0:
                        params = [
                            item["end_date"],
                            result[0],
                            item["fomer_name"],
                            item["gmt_create"],
                            item["user_create"]
                        ]
                        sql = "insert into company_former_name(end_date,company_code,name,gmt_create,user_create)values(%s,%s,%s,%s,%s)"
                        self.cursor.execute(sql, params)
                        self.conn.commit()
            #comapanyStatus
            if isinstance(item, ChinaIntroItem_shenzhen_status):
                if flag6 == 0:
                    flag6 += 1
                    sql_delete = "delete from china_company_status"
                    self.cursor.execute(sql_delete)
                params = [
                    item["ipo_date"],
                    item["code"],
                    item["end_date"],
                    item["gmt_update"],
                    item["user_create"],
                    item["status"],
                    item["exchange_market_code"]
                ]
                sql = "insert into china_company_status(ipo_date,security_code,status_start_date,gmt_update,user_create,status,exchange_market_code)values(%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()


class ChinaFilePipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "china_updateAll_r":
            # if spider.name == "shenzhen_announcement_download":
            if isinstance(item, ChinaIntroItem_ad):
                params = [
                    item["country_code"],
                    item["exchange_market_code"],
                    item["company_code"],
                    item["fiscal_year"],
                    item["disclosure_date"],
                    item["file_name"],
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
                    item["detail_type"]
                ]
                sql = "insert into non_financial_statement_index(country_code,exchange_market_code,company_code,fiscal_year," \
                      "disclosure_date,file_original_title,language_written_code,doc_type,doc_source_url,is_doc_url_direct," \
                      "doc_local_path,doc_downloaded_timestamp,is_downloaded,gmt_create,user_create,report_id," \
                      "Non_financial_announcement_detail_type)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()

            # if spider.name == "shenzhen_delisting_download":
            if isinstance(item, ChinaIntroItem_dd):
                params = [
                    item["exchange_market_code"],
                    item["security_code"],
                    item["disclosure_date"],
                    item["file_name"],
                    item["language_written_code"],
                    item["doc_type"],
                    item["doc_source_url"],
                    item["is_doc_url_direct"],
                    item["doc_local_path"],
                    item["is_downloaded"],
                    item["gmt_create"],
                    item["user_create"],
                    item["report_id"]
                ]
                sql = "insert into shenzhen_delisting_announcement(exchange_market_code,security_code,disclosure_date,pdf_name,language_written_code,doc_type,doc_source_url,is_doc_url_direct,doc_local_path,is_downloaded,gmt_create,user_create,report_id)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()

            # if spider.name == "shenzhen_continuous_supervision":
            if isinstance(item, ChinaIntroItem_cs):
                params = [
                    item["exchange_market_code"],
                    item["security_code"],
                    item["disclosure_date"],
                    item["file_name"],
                    item["language_written_code"],
                    item["doc_type"],
                    item["doc_source_url"],
                    item["is_doc_url_direct"],
                    item["doc_local_path"],
                    item["is_downloaded"],
                    item["gmt_create"],
                    item["user_create"],
                    item["report_id"],
                    item["belong_2_type"]
                ]
                sql = "insert into shenzhen_continuous_supervision(exchange_market_code,security_code,disclosure_date,pdf_name,language_written_code,doc_type,doc_source_url,is_doc_url_direct,doc_local_path,is_downloaded,gmt_create,user_create,report_id,belong_2_type)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()

            # if spider.name == "shenzhen_integrity_file":
            if isinstance(item, ChinaIntroItem_if):
                params = [
                    item["report_id"],
                    item["agency_name"],
                    item["agency_type"],
                    item["security_code"],
                    item["exchange_market_code"],
                    item["file_name"],
                    item["punish_date"],
                    item["litigant"],
                    item["language_written_code"],
                    item["doc_type"],
                    item["punish_type"],
                    item["doc_source_url"],
                    item["is_doc_url_direct"],
                    item["doc_local_path"],
                    item["is_downloaded"],
                    item["belong_2_type"],
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql = "insert into shenzhen_integrity_file(report_id,agency_name,agency_type,security_code,exchange_market_code,pdf_name,punish_date,litigant,language_written_code,doc_type,punish_type,doc_source_url,is_doc_url_direct,doc_local_path,is_downloaded,belong_2_type,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()
        if isinstance(item, ChinaIntroItem_sh) or isinstance(item, ChinaIntroItem_dsn):
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
                item["report_id"],
                item["announcement_type"]
            ]
            sql = "insert into financial_statement_index(country_code,exchange_market_code,company_code,fiscal_year," \
                  "financial_statement_season_type_code,financial_reporting_standard_code,disclosure_date,file_original_title," \
                  "language_written_code,doc_type,doc_source_url,is_doc_url_direct,doc_local_path,doc_downloaded_timestamp," \
                  "is_downloaded,currency_code,gmt_create,user_create,report_id,announcement_type)" \
                  "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql, params)
            self.conn.commit()
        if isinstance(item, ChinaIntroItem_sh_non):
            params = [
                item["country_code"],
                item["exchange_market_code"],
                item["company_code"],
                item["fiscal_year"],
                item["disclosure_date"],
                item["file_name"],
                item["doc_type"],
                item["doc_source_url"],
                item["is_doc_url_direct"],
                item["doc_local_path"],
                item["doc_downloaded_timestamp"],
                item["is_downloaded"],
                item["gmt_create"],
                item["user_create"],
                item["report_id"],
                item["detail_type"],
                item["language_written_code"]
            ]
            sql = "insert into non_financial_statement_index(country_code,exchange_market_code,company_code,fiscal_year," \
                  "disclosure_date,file_original_title,doc_type,doc_source_url,is_doc_url_direct,doc_local_path,doc_downloaded_timestamp," \
                  "is_downloaded,gmt_create,user_create,report_id,Non_financial_announcement_detail_type,language_written_code)" \
                  "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql, params)
            self.conn.commit()
        if spider.name == "for_china_loss":
            params = [
                item["is_downloaded"],
                item["gmt_update"],
                item["doc_downloaded_timestamp"],
                item["user_update"],
                0,
                item["report_id"]
            ]
            sql = "update financial_statement_index set is_downloaded=%s,gmt_update=%s,doc_downloaded_timestamp=%s,user_update=%s,pdf_state=%s where report_id=%s"
            self.cursor.execute(sql, params)
            self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()