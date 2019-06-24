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

    def for_update(self, item, result):
        jud_list = []
        params = []
        name_origin = result[0]
        name_en = result[1]
        security_type = result[2]
        ipo_date = result[3]
        if item["name_origin"] != name_origin:
            jud_list.append("name_origin")
            params.append(item["name_origin"])
        if item["name_en"] != name_en:
            jud_list.append("name_en")
            params.append(item["name_en"])
        if item["security_type"] != security_type:
            jud_list.append("security_type")
            params.append(item["security_type"])
        if str(item["ipo_date"]) != str(ipo_date):
            jud_list.append("ipo_date")
            params.append(item["ipo_date"])
        sql_update = "update securities_list set "
        for temp in range(len(jud_list)):
            sql_update = sql_update + jud_list[temp] + "=%s" + ","
        sql_update = sql_update + " user_update=%s, gmt_update=%s where security_code = %s and country_code_origin = %s"
        params.extend(["zx", item["gmt_create"], item["security_code"], "CHN"])
        self.cursor.execute(sql_update, params)
        self.conn.commit()

    def process_item(self, item, spider):
        if isinstance(item, ShenzhensecuritiesItemS):
            sql_select = "select name_origin,name_en,security_type,ipo_date,code from securities_list where " \
                         "security_code = %s and country_code_origin = %s"
            self.cursor.execute(sql_select, [item["security_code"], "CHN"])
            result = self.cursor.fetchone()
            if result:
                self.for_update(item, result)
            else:
                sql = "select code from company where name_origin = %s"
                data = re.search("\d{4}\u5e74(.+?\u516c\u53f8)", str(item["name_origin"])).group(1)
                self.cursor.execute(sql, data)
                results = self.cursor.fetchone()
                if results:
                    parameter_company = [
                        results[0],
                        item["name_origin"],
                        item["name_en"],
                        item["security_code"],
                        item["country_code_origin"],
                        item["exchange_market_code"],
                        item["ipo_date"],
                        item["gmt_create"],
                        item["user_create"]
                    ]
                    sql_company = "insert into securities_list(code,name_origin,name_en,security_code," \
                                  "country_code_origin,exchange_market_code,ipo_date,gmt_create,user_create)" \
                                  "values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    self.cursor.execute(sql_company, parameter_company)
                    self.conn.commit()

            # 入库securities_profile_definition
            global flag
            if flag == 0:
                flag += 1
                for temp in item:
                    if "_sz" in temp:
                        name = temp
                        display_label = item[temp]
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
                        sql_jud = "select id from securities_profile_definition where name = %s"
                        self.cursor.execute(sql_jud, name)
                        results = self.cursor.fetchall()
                        if len(results) == 0:
                            sql = "insert into securities_profile_definition(name,display_label,data_type,sort,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s)"
                            self.cursor.execute(sql, parameter)
                            self.conn.commit()

            #入库securities_profile_detail
            if result:
                for each in item:
                    if "_sz" in each:
                        name_pd = str(each).replace("_sz", "")
                        sql_select = "select id from securities_profile_definition where name = %s"
                        self.cursor.execute(sql_select, each)
                        result_id = self.cursor.fetchone()
                        company_profile_definition_id = result_id[0]
                        parameter_detail = [
                            item[name_pd],
                            item["gmt_create"],
                            "zx",
                            result[4],
                            company_profile_definition_id
                        ]
                        sql_jud = "select value from securities_profile_detail where company_code = %s and company_profile_definition_id = %s"
                        self.cursor.execute(sql_jud, [result[4], company_profile_definition_id])
                        jud_result = self.cursor.fetchone()
                        if jud_result is not None:
                            if str(item[name_pd]) != str(jud_result[0]):
                                sql_update = "update securities_profile_detail set value = %s, gmt_update=%s, user_create=%s where company_code = %s and company_profile_definition_id = %s"
                                self.cursor.execute(sql_update, parameter_detail)
                                self.conn.commit()
                        else:
                            parameter_d = [
                                company_profile_definition_id,
                                result[4],
                                item[name_pd],
                                item["gmt_create"],
                                "zx"
                            ]
                            sql_insert = "insert into securities_profile_detail(company_profile_definition_id,company_code,value,gmt_create,user_create)values(%s,%s,%s,%s,%s)"
                            self.cursor.execute(sql_insert, parameter_d)
                            self.conn.commit()
        if isinstance(item, ShenzhensecuritiesItem):
            # params = [
            #     item["company_id"],
            #     item["detail_type"],
            #     item["doc_source_url"],
            #     item["disclosure_date"],
            #     item["file_name"],
            #     item["report_id"],
            #     item["doc_local_path"],
            #     item["country_code"],
            #     item["is_doc_url_direct"],
            #     item["is_downloaded"],
            #     item["language_written_code"],
            #     item["gmt_create"],
            #     item["doc_downloaded_timestamp"],
            #     item["user_create"],
            #     item["doc_type"],
            #     item["exchange_market_code"]
            # ]
            # sql = "insert into securities_statement_index(company_code,detail_type,doc_source_url,disclosure_date," \
            #       "file_original_title,report_id,doc_local_path,country_code,is_doc_url_direct,is_downloaded,language_written_code," \
            #       "gmt_create,doc_downloaded_timestamp,user_create,doc_type,exchange_market_code)value(%s,%s,%s,%s,%s,%s,%s,%s," \
            #       "%s,%s,%s,%s,%s,%s,%s,%s)"
            # self.cursor.execute(sql, params)
            # self.conn.commit()
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
            # params = [
            #     item["object"],
            #     item["type"],
            #     item["number"],
            #     item["file_name"],
            #     item["publish_date"],
            #     item["related_bond"],
            #     item["doc_source_url"],
            #     item["belong_2_type"],
            #     item["company_id"],
            #     item["report_id"],
            #     item["doc_local_path"],
            #     item["country_code"],
            #     item["is_doc_url_direct"],
            #     item["is_downloaded"],
            #     item["language_written_code"],
            #     item["gmt_create"],
            #     item["doc_downloaded_timestamp"],
            #     item["user_create"],
            #     item["doc_type"],
            #     item["exchange_market_code"]
            # ]
            # sql = "insert into securities_announcement_szse(object,type,number,title,publish_date,related_bond," \
            #       "doc_source_url,belong_2_type,company_code,report_id,doc_local_path,country_code,is_doc_url_direct," \
            #       "is_downloaded,language_written_code,gmt_create,doc_downloaded_timestamp,user_create,doc_type," \
            #       "exchange_market_code)value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            # self.cursor.execute(sql, params)
            # self.conn.commit()
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

        if spider.name == "bond":
            sql_jud = 'select id from bond_of_china where bond_code = %s'
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
                    item["security_code"]
                ]
                sql = "update bond_of_china set security_form=%s,interest_payment_method=%s,start_interest_date=%s," \
                       "end_date=%s,redemption_date=%s,repayment_period=%s,interest_date_description=%s,delisting_date=%s," \
                       "unit_denomination=%s,interest_rate_type=%s,coupon_rate=%s,interest_rate_start_date=%s," \
                       "interest_rate_end_date=%s,issue_object=%s,issue_price=%s,issue_start_date=%s,issue_end_date=%s," \
                       "actual_circulation=%s,issuance_method=%s,issuance_fee_rate=%s,distribution_method=%s,tender_date=%s,short_name=%s " \
                       "where bond_code=%s"
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
                    item["gmt_create"],
                    item["user_create"]
                ]
                sql = "insert into bond_of_china(security_form,interest_payment_method,start_interest_date,end_date," \
                      "redemption_date,repayment_period,interest_date_description,delisting_date,unit_denomination," \
                      "interest_rate_type,coupon_rate,interest_rate_start_date,interest_rate_end_date,issue_object," \
                      "issue_price,issue_start_date,issue_end_date,actual_circulation,issuance_method,issuance_fee_rate," \
                      "distribution_method,tender_date,bond_code,short_name,bond_full_name_origin,bond_full_name_en," \
                      "bond_type,ipo_date,gmt_create,user_create)value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                      "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                self.cursor.execute(sql, params)
                self.conn.commit()
        return item

    def close_spider(self, spider):
        """Discard the database pool on spider close"""
        self.cursor.close()
        self.conn.close()
