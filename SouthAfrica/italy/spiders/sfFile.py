# -*- coding: utf-8 -*-
import scrapy
import pymysql
import json
import re
import time
import random
from datetime import datetime
from italy.items import ItalyfileItem


class ItalyallSpider(scrapy.Spider):
    name = 'sfFile'
    allowed_domains = ['jse.co.za']
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select code,info_disclosure_id from company where country_code_listed='ZAF'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def forNewst(self, code):
        sql = "select max(disclosure_date) from financial_statement_index where country_code='ZAF' and company_code=%s"
        self.cursor.execute(sql, code)
        result = self.cursor.fetchone()
        if result[0]:
            newstDate = str(result[0])
        else:
            newstDate = "0"
        return newstDate

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def commonItem(self, item):
        item["financial_statement_season_type_code"] = ""
        item["exchange_market_code"] = "JSE"
        item["fiscal_year"] = str(item["disclosure_date"]).split("-")[0]
        item["report_id"] = item["company_code"] + self.uniqueIDMaker()
        item["doc_local_path"] = "/volume3/homes3/ZAF/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".pdf"
        item["country_code"] = "ZAF"
        item["doc_type"] = "pdf"
        item["is_doc_url_direct"] = 1
        item["is_downloaded"] = 1
        item["language_written_code"] = "en"
        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item["doc_downloaded_timestamp"] = item["gmt_create"]
        item["user_create"] = "zx"
        item["file_name"] = item["report_id"]
        return item

    def start_requests(self):
        for temp in self.results:
            newstDate = self.forNewst(temp[0])
            url = "https://www.jse.co.za/_vti_bin/JSE/SENSService.svc/GetSensAnnouncementsByIssuerMasterId"
            sen = {
                "issuerMasterId": str(temp[1])
            }
            yield scrapy.Request(url, method="POST", body=json.dumps(sen), callback=self.parse,
                                 meta={"code": temp[0], "newstDate": newstDate})

            link = "https://www.jse.co.za/_vti_bin/JSE/WebstirService.svc/GetWebstirDocumentYearsByIssuerMasterId"
            rd = {
                "issuerMasterId": str(temp[1])
            }
            yield scrapy.Request(link, method="POST", body=json.dumps(rd), callback=self.rdParse,
                                 meta={"code": temp[0], "id": str(temp[1]), "newstDate": newstDate})

    def parse(self, response):
        newstDate = response.meta["newstDate"]
        data = json.loads(response.text)["GetSensAnnouncementsByIssuerMasterIdResult"]
        for temp in data:
            item = ItalyfileItem()
            item["company_code"] = response.meta["code"]
            item["pdf_name"] = temp["FlashHeadline"]
            item["doc_source_url"] = temp["PDFPath"]
            time_local = time.localtime(int(re.search("Date\((.{10})", str(temp["AcknowledgeDateTime"])).group(1)))
            item["disclosure_date"] = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
            if item["disclosure_date"] > newstDate:
                item = self.commonItem(item)
                yield item

    def rdParse(self, response):
        newstDate = response.meta["newstDate"]
        code = response.meta["code"]
        yearList = json.loads(response.text)["GetWebstirDocumentYearsByIssuerMasterIdResult"]
        for temp in yearList:
            link = "https://www.jse.co.za/_vti_bin/JSE/WebstirService.svc/GetWebstirDocumentsByIssuerMasterIdAndYear"
            rd = {
                "issuerMasterId": response.meta["id"],
                "year": str(temp)
            }
            yield scrapy.Request(link, method="POST", body=json.dumps(rd), callback=self.rdParseDetail,
                                 meta={"code": code, "newstDate": newstDate})

    def rdParseDetail(self, response):
        newstDate = response.meta["newstDate"]
        data = json.loads(response.text)["GetWebstirDocumentsByIssuerMasterIdAndYearResult"]
        for temp in data:
            item = ItalyfileItem()
            item["company_code"] = response.meta["code"]
            item["pdf_name"] = temp["DocumentType"]
            item["doc_source_url"] = temp["DocumentUrl"]
            time_local = time.localtime(int(re.search("Date\((.{10})", str(temp["SubmittedDate"])).group(1)))
            item["disclosure_date"] = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
            if item["disclosure_date"] > newstDate:
                item = self.commonItem(item)
                yield item