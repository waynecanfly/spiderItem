# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
from GBR.items import GbrItem
from GBR.spiderAPI import uniqueID


class AnnouncementspiderSpider(scrapy.Spider):
    name = 'fileDownload'
    allowed_domains = ['morningstar.co.uk', "baidu.com", "data5u.com"]
    start_urls = ["https://www.baidu.com/"]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select doc_source_url,fiscal_year,financial_statement_season_type_code,disclosure_date," \
          "file_original_title,announcement_type,detail_type,jud_type,company_name from gbr_not_parsing group by doc_source_url"
    cursor.execute(sql)
    results = cursor.fetchall()

    def paramsFunc(self, item):
        item["report_id"] = item["company_code"] + uniqueID.uniqueIDMaker()
        jud = str(item["doc_source_url"]).split("=")[-1]
        if len(jud) == 9 or len(jud) == 8:
            item["doc_type"] = "pdf"
            item["doc_local_path"] = "/volume3/homes3/GBR/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".pdf"
        else:
            item["doc_type"] = "html"
            item["doc_local_path"] = "/volume3/homes3/GBR/" + str(item["fiscal_year"]) + "/" + item[
                "report_id"] + ".html"
        item["country_code"] = "GBR"
        item["is_doc_url_direct"] = 1
        item["is_downloaded"] = 1
        item["language_written_code"] = "en"
        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item["doc_downloaded_timestamp"] = item["gmt_create"]
        item["user_create"] = "zx"
        item["file_name"] = jud
        return item

    def parse(self, response):
        for temp in self.results:
            item = GbrItem()
            company_name = temp[8]
            sql = "select code from company where country_code_listed='GBR' and name_origin=%s"
            self.cursor.execute(sql, company_name)
            result = self.cursor.fetchone()
            if result:
                item["company_code"] = result[0]
                item["doc_source_url"] = temp[0]
                item["detail_type"] = temp[6]
                item["financial_statement_season_type_code"] = temp[2]
                item["announcement_type"] = temp[5]
                source = temp[7]
                item["disclosure_date"] = temp[3]
                item["fiscal_year"] = temp[1]
                item["pdf_name"] = temp[4]
                item["exchange_market_code"] = source
                item = self.paramsFunc(item)
                yield item
