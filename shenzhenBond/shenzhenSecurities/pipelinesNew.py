# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
import re
from shenzhenSecurities.items import ShenzhensecuritiesItemS, ShenzhensecuritiesItem, ShenzhensecuritiesItemOther

flag = 0


class ShenzhensecuritiesListPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if spider.name == "bond":
            sql = "select code from company where name_origin = %s and country_code_listed = 'CHN'"
            try:
                data = re.search("\d{4}\u5e74(.+?\u516c\u53f8)", str(item["name_origin"])).group(1)
                self.cursor.execute(sql, data)
                results = self.cursor.fetchone()
                if results:
                    code = results[0]
                else:
                    code = ""
                sql_jud = 'select id from bond_of_china where bond_code = %s and user_create="zx"'
                self.cursor.execute(sql_jud, item["security_code"])
                result = self.cursor.fetchall()
                if len(result) != 0:
                    params = [
                        item["security_form"],
                        item["interest_payment_method"],
                        item["start_interest_date"],
                        item["end_date"],
                        item["redemption_date"],
                        item["repayment_period"],
                        item["interest_date_description"],
                        item["delisting_date"],
                        item["unit_denomination"],
                        item["interest_rate_type"],
                        item["coupon_rate"],
                        item["interest_rate_start_date"],
                        item["interest_rate_end_date"],
                        item["issue_object"],
                        item["issue_price"],
                        item["issue_start_date"],
                        item["issue_end_date"],
                        item["actual_circulation"],
                        item["issuance_method"],
                        item["issuance_fee_rate"],
                        item["distribution_method"],
                        item["tender_date"],
                        item["short_name"],
                        item["name_origin"],
                        item["name_en"],
                        item["security_type"],
                        item["ipo_date"],
                        code,
                        item["gmt_create"],
                        "zx",
                        item["security_code"],
                        "zx"
                    ]
                    sql = "update bond_of_china set security_form=%s,interest_payment_method=%s,start_interest_date=%s," \
                           "end_date=%s,redemption_date=%s,repayment_period=%s,interest_date_description=%s,delisting_date=%s," \
                           "unit_denomination=%s,interest_rate_type=%s,coupon_rate=%s,interest_rate_start_date=%s," \
                           "interest_rate_end_date=%s,issue_object=%s,issue_price=%s,issue_start_date=%s,issue_end_date=%s," \
                           "actual_circulation=%s,issuance_method=%s,issuance_fee_rate=%s,distribution_method=%s," \
                          "tender_date=%s,short_name=%s,bond_full_name_origin=%s,bond_full_name_en=%s,bond_type=%s,ipo_date=%s,company_code=%s," \
                          "gmt_update=%s,user_update=%s where bond_code=%s and user_create=%s"
                    self.cursor.execute(sql, params)
                    self.conn.commit()
                else:
                    params = [
                        item["security_form"],
                        item["interest_payment_method"],
                        item["start_interest_date"],
                        item["end_date"],
                        item["redemption_date"],
                        item["repayment_period"],
                        item["interest_date_description"],
                        item["delisting_date"],
                        item["unit_denomination"],
                        item["interest_rate_type"],
                        item["coupon_rate"],
                        item["interest_rate_start_date"],
                        item["interest_rate_end_date"],
                        item["issue_object"],
                        item["issue_price"],
                        item["issue_start_date"],
                        item["issue_end_date"],
                        item["actual_circulation"],
                        item["issuance_method"],
                        item["issuance_fee_rate"],
                        item["distribution_method"],
                        item["tender_date"],
                        item["security_code"],
                        item["short_name"],
                        item["name_origin"],
                        item["name_en"],
                        item["security_type"],
                        item["ipo_date"],
                        code,
                        item["gmt_create"],
                        item["user_create"]
                    ]
                    sql = "insert into bond_of_china(security_form,interest_payment_method,start_interest_date,end_date," \
                          "redemption_date,repayment_period,interest_date_description,delisting_date,unit_denomination," \
                          "interest_rate_type,coupon_rate,interest_rate_start_date,interest_rate_end_date,issue_object," \
                          "issue_price,issue_start_date,issue_end_date,actual_circulation,issuance_method,issuance_fee_rate," \
                          "distribution_method,tender_date,bond_code,short_name,bond_full_name_origin,bond_full_name_en," \
                          "bond_type,ipo_date,company_code,gmt_create,user_create)value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                          "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    self.cursor.execute(sql, params)
                    self.conn.commit()
            except:
                print(item["name_origin"])
        if spider.name == "add_szse_bond":
            sql_jud = 'select bond_full_name_origin,bond_full_name_en from bond_of_china where bond_code = %s'
            self.cursor.execute(sql_jud, item["bond_code"])
            result = self.cursor.fetchall()
            if len(result) == 0:
                params = [
                    item["bond_code"],
                    item['short_name'],
                    item["ipo_date"],
                    item["start_interest_date"],
                    item["end_date"],
                    item["coupon_rate"],
                    item["actual_circulation"],
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql_insert = "insert into bond_of_china(bond_code, short_name,ipo_date,start_interest_date,end_date," \
                             "coupon_rate,actual_circulation,gmt_create,user_create)value(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql_insert, params)
                self.conn.commit()
            else:
                if result[0][0] == None and result[0][1] == None:
                    params = [
                        item['short_name'],
                        item["ipo_date"],
                        item["start_interest_date"],
                        item["end_date"],
                        item["coupon_rate"],
                        item["actual_circulation"],
                        item["gmt_create"],
                        item["user_create"],
                        item["bond_code"],
                        "zx"
                    ]
                    sql_update = "update bond_of_china set short_name=%s,ipo_date=%s,start_interest_date=%s," \
                                 "end_date=%s,coupon_rate=%s,actual_circulation=%s,gmt_update=%s,user_update=%s where" \
                                 " bond_code=%s and user_create=%s"
                    self.cursor.execute(sql_update, params)
                    self.conn.commit()

        if isinstance(item, ShenzhensecuritiesItem):
            params = [
                item["detail_type"],
                item["doc_source_url"],
                item["disclosure_date"],
                item["file_name"],
                item["report_id"],
                item["doc_local_path"],
                item["country_code"],
                item["is_doc_url_direct"],
                item["is_downloaded"],
                item["gmt_create"],
                item["doc_downloaded_timestamp"],
                item["user_create"],
                item["doc_type"],
                item["exchange_market_code"],
                item["bond_short_name"]
            ]
            sql = "insert into securities_statement_index(detail_type,doc_source_url,disclosure_date," \
                  "file_original_title,report_id,doc_local_path,country_code,is_doc_url_direct,is_downloaded," \
                  "gmt_create,doc_downloaded_timestamp,user_create,doc_type,exchange_market_code,bond_short_name)value(%s,%s,%s,%s,%s,%s," \
                  "%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql, params)
            self.conn.commit()

        if isinstance(item, ShenzhensecuritiesItemOther):
            params = [
                item["object"],
                item["type"],
                item["number"],
                item["file_name"],
                item["publish_date"],
                item["related_bond"],
                item["doc_source_url"],
                item["belong_2_type"],
                item["report_id"],
                item["doc_local_path"],
                item["country_code"],
                item["is_doc_url_direct"],
                item["is_downloaded"],
                item["gmt_create"],
                item["doc_downloaded_timestamp"],
                item["user_create"],
                item["doc_type"],
                item["exchange_market_code"]
            ]
            sql = "insert into securities_announcement_szse(object,type,number,title,publish_date,related_bond," \
                  "doc_source_url,belong_2_type,report_id,doc_local_path,country_code,is_doc_url_direct," \
                  "is_downloaded,gmt_create,doc_downloaded_timestamp,user_create,doc_type," \
                  "exchange_market_code)value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql, params)
            self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()
