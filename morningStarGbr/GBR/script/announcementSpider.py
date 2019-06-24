# -*- coding: utf-8 -*-
import scrapy
import pymysql
import re
import time
from GBR.items import GbrItem
from GBR.spiderAPI import forQuery, uniqueID
from urllib.parse import quote


class AnnouncementspiderSpider(scrapy.Spider):
    name = 'announcementSpider'
    allowed_domains = ['morningstar.co.uk']
    start_urls = ["http://tools.morningstar.co.uk/tsweu6nqxu/globaldocuments/list/default.aspx"]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select name_origin,code from company where country_code_listed='GBR' and id=32799"
    cursor.execute(sql)
    result = cursor.fetchall()
    qData = forQuery.get_params()

    def parse(self, response):
        for temp in self.result:
            company_name = temp[0]
            company_id = temp[1]
            for each in self.qData:
                num = 1
                detail_type = each["Dt"]
                financial_statement_season_type_code = each["St"]
                announcement_type = each["At"]
                view = response.xpath('//div/input[@id="__VIEWSTATE"]/@value').extract()[0]
                token = response.xpath('//input[@id="ctl00___RequestVerificationToken"]/@value').extract()[0]
                data = {
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "__LASTFOCUS": "",
                    "__VIEWSTATE": view,
                    "__VIEWSTATEGENERATOR": "75374BBC",
                    "__VIEWSTATEENCRYPTED": "",
                    "ctl00$ContentPlaceHolder1$ddlHeadlineType": each["Qt"],
                    "ctl00$ContentPlaceHolder1$txtDateFrom": "01/01/2008",
                    "ctl00$ContentPlaceHolder1$txtDateTo": "20/09/2018",
                    "ctl00$ContentPlaceHolder1$btnGo": "Go",
                    "ctl00$ContentPlaceHolder1$txtCompany": company_name,
                    "ctl00$ContentPlaceHolder1$txtLei": "",
                    "ctl00$ContentPlaceHolder1$ddlEeapCategory": "null",
                    "ctl00$ContentPlaceHolder1$ddlPageSize": "20",
                    "ctl00$ContentPlaceHolder1$hdnExitingText": "You are about to leave the National Storage Mechanism",
                    "ctl00$__RequestVerificationToken": token
                }
                yield scrapy.FormRequest.from_response(response, formdata=data, callback=self.AnalyFunc,
                                                       meta={"company_id": company_id, "detail_type": detail_type,
                                                             "financial_statement_season_type_code": financial_statement_season_type_code,
                                                             "announcement_type": announcement_type, "num": num, "Qt": each["Qt"],
                                                             "company_name": company_name})

    def AnalyFunc(self, response):
        page = response.meta["num"]
        data_list = re.findall('class="gridDocumentsDate"><a>(.*?)</a>.*?class="gridDocumentsTime"><a>(.*?)</a>.*?class='
                               '"gridDocumentsSource"><a>(\w+?)</a>.*?href="(/tsweu6nqxu/.*?DocumentId=\d+?)"', str(response.body))
        company_name = response.meta["company_name"]
        Qt = response.meta["Qt"]
        print("%s,%s,%s,%s" % (page, len(data_list), company_name, Qt))
        company_code = response.meta["company_id"]
        detail_type = response.meta["detail_type"]
        financial_statement_season_type_code = response.meta["financial_statement_season_type_code"]
        announcement_type = response.meta["announcement_type"]
        for temp in data_list:
            item = GbrItem()
            item["company_code"] = company_code
            item["detail_type"] = detail_type
            item["financial_statement_season_type_code"] = financial_statement_season_type_code
            item["announcement_type"] = announcement_type
            date = temp[0]
            source = temp[2]
            if source == "RNS":
                sql = "select min(disclosure_date) from financial_statement_index where country_code = 'GBR' and company_code = %s"
                self.cursor.execute(sql, item["company_code"])
                result = self.cursor.fetchone()
                if result:
                    try:
                        minDate = int(str(result[0]).replace("-", "").replace(" ", "").replace(":", ""))
                        item["disclosure_date"] = str(date).split("/")[-1] + "-" + str(date).split("/")[1] + "-" + \
                                                  str(date).split("/")[0] + " " + temp[1]
                        nowDate = int(str(item["disclosure_date"]).replace("-", "").replace(" ", "").replace(":", ""))
                        if nowDate < minDate:
                            link = temp[3]
                            item["doc_source_url"] = "http://tools.morningstar.co.uk" + link
                            item["report_id"] = item["company_code"] + uniqueID.uniqueIDMaker()
                            item["exchange_market_code"] = source
                            item["fiscal_year"] = str(date).split("/")[-1]
                            jud = str(item["doc_source_url"]).split("=")[-1]
                            if len(jud) == 9:
                                item["doc_type"] = "pdf"
                            else:
                                item["doc_type"] = "html"
                            item["doc_local_path"] = "/volum3/homes3/GBR/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".pdf"
                            item["country_code"] = "GBR"
                            item["is_doc_url_direct"] = 1
                            item["is_downloaded"] = 1
                            item["language_written_code"] = "en"
                            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                            item["doc_downloaded_timestamp"] = item["gmt_create"]
                            item["user_create"] = "zx"
                            item["file_name"] = jud
                            # yield item
                    except:
                        item["disclosure_date"] = str(date).split("/")[-1] + "-" + str(date).split("/")[1] + "-" + \
                                                  str(date).split("/")[0] + " " + temp[1]
                        link = temp[3]
                        item["doc_source_url"] = "http://tools.morningstar.co.uk" + link
                        item["report_id"] = item["company_code"] + uniqueID.uniqueIDMaker()
                        item["exchange_market_code"] = source
                        item["fiscal_year"] = str(date).split("/")[-1]
                        jud = str(item["doc_source_url"]).split("=")[-1]
                        if len(jud) == 9:
                            item["doc_type"] = "pdf"
                        else:
                            item["doc_type"] = "html"
                        item["doc_local_path"] = "/volum3/homes3/GBR/" + str(item["fiscal_year"]) + "/" + item[
                            "report_id"] + ".pdf"
                        item["country_code"] = "GBR"
                        item["is_doc_url_direct"] = 1
                        item["is_downloaded"] = 1
                        item["language_written_code"] = "en"
                        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        item["doc_downloaded_timestamp"] = item["gmt_create"]
                        item["user_create"] = "zx"
                        item["file_name"] = jud
                        # yield item
                else:
                    item["disclosure_date"] = str(date).split("/")[-1] + "-" + str(date).split("/")[1] + "-" + \
                                              str(date).split("/")[0] + " " + temp[1]
                    link = temp[3]
                    item["doc_source_url"] = "http://tools.morningstar.co.uk" + link
                    item["report_id"] = item["company_code"] + uniqueID.uniqueIDMaker()
                    item["exchange_market_code"] = source
                    item["fiscal_year"] = str(date).split("/")[-1]
                    jud = str(item["doc_source_url"]).split("=")[-1]
                    if len(jud) == 9:
                        item["doc_type"] = "pdf"
                    else:
                        item["doc_type"] = "html"
                    item["doc_local_path"] = "/volum3/homes3/GBR/" + str(item["fiscal_year"]) + "/" + item[
                        "report_id"] + ".pdf"
                    item["country_code"] = "GBR"
                    item["is_doc_url_direct"] = 1
                    item["is_downloaded"] = 1
                    item["language_written_code"] = "en"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["doc_downloaded_timestamp"] = item["gmt_create"]
                    item["user_create"] = "zx"
                    item["file_name"] = jud
                    # yield item
            else:
                item["disclosure_date"] = str(date).split("/")[-1] + "-" + str(date).split("/")[1] + "-" + str(date).split("/")[0] + " " + temp[1]
                link = temp[3]
                item["doc_source_url"] = "http://tools.morningstar.co.uk" + link
                item["report_id"] = item["company_code"] + uniqueID.uniqueIDMaker()
                item["exchange_market_code"] = source
                item["fiscal_year"] = str(date).split("/")[-1]
                jud = str(item["doc_source_url"]).split("=")[-1]
                if len(jud) == 9:
                    item["doc_type"] = "pdf"
                else:
                    item["doc_type"] = "html"
                item["doc_local_path"] = "/volum3/homes3/GBR/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".pdf"
                item["country_code"] = "GBR"
                item["is_doc_url_direct"] = 1
                item["is_downloaded"] = 1
                item["language_written_code"] = "en"
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["doc_downloaded_timestamp"] = item["gmt_create"]
                item["user_create"] = "zx"
                item["file_name"] = jud
                # yield item
        if len(data_list) == 20:
            page += 1
            view = response.xpath('//div/input[@id="__VIEWSTATE"]/@value').extract()[0]
            token = response.xpath('//input[@id="ctl00___RequestVerificationToken"]/@value').extract()[0]
            data = {
                "__EVENTTARGET": "ctl00$ContentPlaceHolder1$pgrDocumentList",
                "__EVENTARGUMENT": str(page),
                "__LASTFOCUS": "",
                "__VIEWSTATE": view,
                "__VIEWSTATEGENERATOR": "75374BBC",
                "__VIEWSTATEENCRYPTED": "",
                "ctl00$ContentPlaceHolder1$ddlHeadlineType": Qt,
                "ctl00$ContentPlaceHolder1$txtDateFrom": "01/01/2008",
                "ctl00$ContentPlaceHolder1$txtDateTo": "20/09/2018",
                "ctl00$ContentPlaceHolder1$txtCompany": company_name,
                "ctl00$ContentPlaceHolder1$txtLei": "",
                "ctl00$ContentPlaceHolder1$ddlEeapCategory": "null",
                "ctl00$ContentPlaceHolder1$ddlPageSize": "20",
                "ctl00$ContentPlaceHolder1$hdnExitingText": "You are about to leave the National Storage Mechanism",
                "ctl00$__RequestVerificationToken": token
            }
            yield scrapy.FormRequest.from_response(response, formdata=data, callback=self.AnalyFunc,
                                                   meta={"company_id": company_code, "detail_type": detail_type,
                                                         "financial_statement_season_type_code": financial_statement_season_type_code,
                                                         "announcement_type": announcement_type, "num": page, "company_name": company_name,
                                                         "Qt": Qt})