# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
import random
from datetime import datetime
from HCK_l.items import ChinaIntroItem


class CodeSpider(scrapy.Spider):
    name = 'HCK_pdf_spider'
    allowed_domains = ['hkexnews.hk']
    # pattern = re.compile("\d{4}")
    query_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    start_urls = ["http://www.hkexnews.hk/listedco/listconews/advancedsearch/search_active_main.aspx"]
    code_list = []
    report_num_dict = {}
    doc_type_list = [".xls", ".XLS"]
    post_type = [
        {"tier_1": "1", "tier_2": "-2", "type1": 0, "type2": "8", "type3": ""},
        {"tier_1": "2", "tier_2": "-2", "type1": 0, "type2": "9", "type3": ""},
        {"tier_1": "3", "tier_2": "-2", "type1": 0, "type2": "10", "type3": ""},
        {"tier_1": "13", "tier_2": "-2", "type1": 0, "type2": "11", "type3": ""},
        {"tier_1": "14", "tier_2": "-2", "type1": 0, "type2": "12", "type3": ""},
        {"tier_1": "7", "tier_2": "-2", "type1": 0, "type2": "13", "type3": ""},
        {"tier_1": "11", "tier_2": "-2", "type1": 0, "type2": "14", "type3": ""},
        {"tier_1": "12", "tier_2": "-2", "type1": 0, "type2": "15", "type3": ""},
        {"tier_1": "5", "tier_2": "-2", "type1": 0, "type2": "16", "type3": ""},
        {"tier_1": "9", "tier_2": "-2", "type1": 0, "type2": "17", "type3": ""},
        {"tier_1": "8", "tier_2": "-2", "type1": 0, "type2": "18", "type3": ""},
        {"tier_1": "10", "tier_2": "-2", "type1": 0, "type2": "19", "type3": ""},
        {"tier_1": "15", "tier_2": "-2", "type1": 0, "type2": "20", "type3": ""},
        {"tier_1": "16", "tier_2": "-2", "type1": 0, "type2": "21", "type3": ""},
        {"tier_1": "17", "tier_2": "-2", "type1": 0, "type2": "22", "type3": ""},
        {"tier_1": "4", "tier_2": "159", "type1": 1, "type2": "", "type3": "FY"},
        {"tier_1": "4", "tier_2": "161", "type1": 1, "type2": "", "type3": "Q"},
        {"tier_1": "4", "tier_2": "211", "type1": 1, "type2": "", "type3": "ESG"},
        {"tier_1": "4", "tier_2": "160", "type1": 1, "type2": "", "type3": "HY"}
                 ]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,company_id from company_data_source where company_id like 'HKG%'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def jud_season_num(self, report_type_jud):
        """判断report类型对应编号"""
        if report_type_jud == "FY":
            season_num = "06"
        elif report_type_jud == "HY":
            season_num = "08"
        elif report_type_jud == "Q":
            season_num = "05"
        elif report_type_jud == "ESG":
            season_num = "09"
        else:
            season_num = "07"
        return season_num

    def parse(self, response):
        end_time = str(self.query_time).split(" ")[0].replace("-", "")
        end_year = str(self.query_time).split(" ")[0].split("-")[0]
        end_month = str(self.query_time).split(" ")[0].split("-")[1]
        end_date = str(self.query_time).split(" ")[0].split("-")[2]
        for temp in self.results:
            code = temp[0]
            company_code = temp[1]
            sql_company = "select currency_code from company where code =%s"
            self.cursor.execute(sql_company, company_code)
            currency_code = self.cursor.fetchone()[0]
            for each in self.post_type:
                flag = 0
                financial_statement_season_type_code = each["type3"]
                detail_type = each["type2"]
                announcement_type = each["type1"]
                if financial_statement_season_type_code != "":
                    sql_select = "select disclosure_date,doc_source_url from financial_statement_index where " \
                                 "disclosure_date in(select max(disclosure_date) as disclosure_date from " \
                                 "financial_statement_index where country_code='HKG' and company_code=%s and " \
                                 "financial_statement_season_type_code=%s) and country_code='HKG' and " \
                                 "company_code=%s and financial_statement_season_type_code=%s"
                    self.cursor.execute(sql_select, [company_code, each["type3"],
                                                     company_code, each["type3"]])
                    results = self.cursor.fetchall()
                    if len(results) != 0:
                        newstdate = results[0][0]
                        newsturl = [i[1] for i in results]
                    else:
                        newstdate = ""
                        newsturl = []
                else:
                    sql_select = "select disclosure_date,doc_source_url from non_financial_statement_index where " \
                                 "disclosure_date in(select max(disclosure_date) as disclosure_date from " \
                                 "non_financial_statement_index where country_code='HKG' and company_code=%s and " \
                                 "Non_financial_announcement_detail_type=%s) and country_code='HKG' and " \
                                 "company_code=%s and Non_financial_announcement_detail_type=%s"
                    self.cursor.execute(sql_select, [company_code, each["type2"],
                                                     company_code, each["type2"]])
                    results = self.cursor.fetchall()
                    if len(results) != 0:
                        newstdate = str(results[0][0])
                        newsturl = [i[1] for i in results]
                    else:
                        newstdate = ""
                        newsturl = []
                __VIEWSTATE = response.xpath('//input[@id="__VIEWSTATE"]/@value').extract()[0]
                __VIEWSTATEGENERATOR = response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').extract()[0]
                __VIEWSTATEENCRYPTED = response.xpath('//input[@id="__VIEWSTATEENCRYPTED"]/@value').extract()[0]
                data = {
                    "__VIEWSTATE": __VIEWSTATE,
                    "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
                    "__VIEWSTATEENCRYPTED": __VIEWSTATEENCRYPTED,
                    "ctl00$txt_today": end_time,
                    "ctl00$hfStatus": "AEM",
                    "ctl00$hfAlert": "",
                    "ctl00$txt_stock_code": code,
                    "ctl00$txt_stock_name": "",
                    "ctl00$rdo_SelectDocType": "rbAfter2006",
                    "ctl00$sel_tier_1": each["tier_1"],
                    "ctl00$sel_DocTypePrior2006": "-1",
                    "ctl00$sel_tier_2_group": "-2",
                    "ctl00$sel_tier_2": each["tier_2"],
                    "ctl00$ddlTierTwo": "176,5,22",
                    "ctl00$ddlTierTwoGroup": "22,5",
                    "ctl00$txtKeyWord": "",
                    "ctl00$rdo_SelectDateOfRelease": "rbManualRange",
                    "ctl00$sel_DateOfReleaseFrom_d": "01",
                    "ctl00$sel_DateOfReleaseFrom_m": "01",
                    "ctl00$sel_DateOfReleaseFrom_y": "2008",
                    "ctl00$sel_DateOfReleaseTo_d": end_date,
                    "ctl00$sel_DateOfReleaseTo_m": end_month,
                    "ctl00$sel_DateOfReleaseTo_y": end_year,
                    "ctl00$sel_defaultDateRange": "SevenDays",
                    "ctl00$rdo_SelectSortBy": "rbDateTime"
                }
                yield scrapy.FormRequest.from_response(
                        response,
                        formdata=data,
                        callback=self.parse2,
                        meta={
                            "company_code": company_code,
                            "financial_statement_season_type_code": financial_statement_season_type_code,
                            "detail_type": detail_type,
                            "announcement_type": announcement_type,
                            "currency_code": currency_code,
                            "code": code, "newstdate": newstdate, "newsturl": newsturl, "flag": flag
                        })

    def parse2(self, response):
        flag = response.meta["flag"]
        newstdate = response.meta["newstdate"]
        newsturl = response.meta["newsturl"]
        code = response.meta["code"]
        company_code = response.meta["company_code"]
        financial_statement_season_type_code = response.meta["financial_statement_season_type_code"]
        detail_type = response.meta["detail_type"]
        announcement_type = response.meta["announcement_type"]
        currency_code = response.meta["currency_code"]
        data_list = response.xpath('//table[@id="ctl00_gvMain"]//tr[starts-with(@style, "color:Black")]')
        jud_page = response.xpath('//tr/td[2]/input[@id="ctl00_btnNext2"]/@src')
        for temp in data_list:
            item = ChinaIntroItem()
            disclosure_date = temp.xpath('./td[1]/span/text()').extract()
            try:
                if len(disclosure_date) != 0:
                    item["disclosure_date"] = str(disclosure_date[0]).split("/")[-1] + "-" + \
                                              str(disclosure_date[0]).split("/")[1] + "-" + \
                                              str(disclosure_date[0]).split("/")[0] + " " + \
                                              str(disclosure_date[-1]) + ":00"
                    item["fiscal_year"] = str(disclosure_date[0]).split("/")[-1]
                    fiscal_year = item["fiscal_year"]
                else:
                    item["disclosure_date"] = ""
                    item["fiscal_year"] = ""
                    fiscal_year = "0000"
                item["file_name"] = temp.xpath('./td[4]/a/text()').extract()[0]
                item["doc_source_url"] = "http://www.hkexnews.hk" + temp.xpath('./td[4]/a/@href').extract()[0]
                if item["doc_source_url"] not in newsturl and item["disclosure_date"] >= newstdate:
                    item["exchange_market_code"] = "HKEX"
                    item["company_code"] = company_code
                    item["financial_statement_season_type_code"] = financial_statement_season_type_code
                    item["announcement_type"] = announcement_type
                    item["detail_type"] = detail_type
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    if any(i in item["doc_source_url"] for i in self.doc_type_list):
                        item["doc_type"] = "excel"
                        item["doc_local_path"] = "/volum3/homes3/HongKong/" + str(fiscal_year) + "/" + item[
                            "report_id"] + ".xls"
                    else:
                        item["doc_type"] = "pdf"
                        item["doc_local_path"] = "/volume3/homes3/HongKong/" + str(fiscal_year) + "/" + item[
                            "report_id"] + ".pdf"
                    item["country_code"] = "HKG"
                    item["is_doc_url_direct"] = 1
                    item["financial_reporting_standard_code"] = "CAS"
                    item["is_downloaded"] = 1
                    item["currency_code"] = currency_code
                    item["language_written_code"] = "en"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["doc_downloaded_timestamp"] = item["gmt_create"]
                    item["user_create"] = "zx"
                    yield item
                else:
                    flag += 1
                    break
            except:
                print("%s,%s,%s" % (len(data_list), code, detail_type))
        if flag == 0:
            if len(jud_page) != 0:
                __VIEWSTATE = response.xpath('//input[@id="__VIEWSTATE"]/@value').extract()[0]
                __VIEWSTATEGENERATOR = response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').extract()[0]
                __VIEWSTATEENCRYPTED = response.xpath('//input[@id="__VIEWSTATEENCRYPTED"]/@value').extract()[0]
                data = {
                    "__VIEWSTATE": __VIEWSTATE,
                    "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
                    "__VIEWSTATEENCRYPTED": __VIEWSTATEENCRYPTED,
                    "ctl00$btnNext2.x": "36",
                    "ctl00$btnNext2.y": "2"
                }
                yield scrapy.FormRequest.from_response(
                    response,
                    formdata=data,
                    callback=self.parse2,
                    meta={
                        "company_code": company_code,
                        "financial_statement_season_type_code": financial_statement_season_type_code,
                        "detail_type": detail_type,
                        "announcement_type": announcement_type,
                        "currency_code": currency_code,
                        "code": code, "newstdate": newstdate, "newsturl": newsturl, "flag": flag
                    })