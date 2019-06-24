# -*- coding: utf-8 -*-
import scrapy
import time
import random
import pymysql
from datetime import datetime
from urllib.parse import quote
from italy.items import ItalyfileItem


class ItalyallSpider(scrapy.Spider):
    name = 'italyFile'
    allowed_domains = ['borsaitaliana.it']
    url1 = "https://www.borsaitaliana.it/borsa/azioni/documenti/societa-quotate/documenti.html?lang=en&data_efficacia_min=&companyName="
    url2 = "&type="
    url3 = "&ndg="
    url4 = "&startingDay=&startingMonth=&startingYear="

    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()

    type = [
        {"dt": "Acquisitions/Disposals", "id": "623", "st": ""},
        {"dt": "Adherence to Codes of Conduct", "id": "624", "st": ""},
        {"dt": "Amendments to the instrument of incorporation", "id": "625", "st": ""},
        {"dt": "Annual Financial Report", "id": "", "st": "FY"},
        {"dt": "Half Yearly Financial Report", "id": "", "st": "HY"},
        {"dt": "Information Published in the previous 12 months", "id": "628", "st": ""},
        {"dt": "Interim Management Reports", "id": "629", "st": ""},
        {"dt": "Issue of Bonds", "id": "630", "st": ""},
        {"dt": "Lists of Candidates for Appointment of Members of Company Bodies", "id": "631", "st": ""},
        {"dt": "Measures pursuant to Article 2446 of the Italian Civil Code", "id": "632", "st": ""},
        {"dt": "Mergers/Spin-offs", "id": "633", "st": ""},
        {"dt": "Other Disclosure", "id": "634", "st": ""},
        {"dt": "Purchase and Sale of Treasury Shares", "id": "635", "st": ""},
        {"dt": "Share Capital Increases", "id": "636", "st": ""},
        {"dt": "Stock Option Plans", "id": "637", "st": ""},
        {"dt": "Transactions with related parties", "id": "638", "st": ""}
    ]

    def forNewst(self, code, season_type, detail_type):
        if detail_type == "":
            sql = "select disclosure_date, doc_source_url from financial_statement_index where disclosure_date in " \
                  "(select max(disclosure_date) as disclosure_date from financial_statement_index where company_code=%s" \
                  "and financial_statement_season_type_code=%s) and company_code=%s and financial_statement_season_type_code=%s"
            self.cursor.execute(sql, [code, season_type, code, season_type])
            results = self.cursor.fetchall()
            if len(results) != 0:
                newstDate = str(results[0][0])
                newstUrl = [i[1] for i in results]
            else:
                newstDate = "0"
                newstUrl = []
        else:
            sql = "select disclosure_date, doc_source_url from non_financial_statement_index where disclosure_date in " \
                  "(select max(disclosure_date) as disclosure_date from non_financial_statement_index where company_code=%s" \
                  "and Non_financial_announcement_detail_type=%s) and company_code=%s and Non_financial_announcement_detail_type=%s"
            self.cursor.execute(sql, [code, detail_type, code, detail_type])
            results = self.cursor.fetchall()
            if len(results) != 0:
                newstDate = str(results[0][0])
                newstUrl = [i[1] for i in results]
            else:
                newstDate = "0"
                newstUrl = []
        return newstDate, newstUrl

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def commonItem(self, item):
        item["exchange_market_code"] = "Borsa Italiana"
        item["fiscal_year"] = str(item["disclosure_date"]).split("-")[0]
        item["report_id"] = item["company_code"] + self.uniqueIDMaker()
        item["doc_local_path"] = "/volume3/homes3/Italy/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".pdf"
        item["country_code"] = "ITA"
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
        url = "https://www.borsaitaliana.it/borsa/azioni/documenti/societa-quotate/documenti.html?lang=en"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        op = response.xpath('//select[@name="ndg"]/option')[1:]
        for temp in op:
            name = temp.xpath('./text()').extract()[0]
            value = temp.xpath('./@value').extract()[0]
            sql = "select code from company where name_origin=%s"
            self.cursor.execute(sql, name)
            result = self.cursor.fetchone()
            if result:
                for each in self.type:
                    season_type = each["st"]
                    detail_type = each["id"]
                    data = self.forNewst(result[0], season_type, detail_type)
                    url = self.url1 + quote(name, 'utf-8') + self.url2 + quote(each["dt"], 'utf-8') + self.url3 + \
                          quote(value, 'utf-8') + self.url4
                    yield scrapy.Request(url, callback=self.fileParse,
                                         meta={"season_type": season_type, "detail_type": detail_type,
                                               "company_code": result[0], "newstDate": data[0], "newstUrl": data[1]})

    def fileParse(self, response):
        newstDate = response.meta["newstDate"]
        newstUrl = response.meta["newstUrl"]
        tr_list = response.xpath('//table[@class="table_dati"]//tr')
        if len(tr_list) != 0:
            tr_list = tr_list[1:]
            for temp in tr_list:
                item = ItalyfileItem()
                item["company_code"] = response.meta["company_code"]
                item["detail_type"] = response.meta["detail_type"]
                item["financial_statement_season_type_code"] = response.meta["season_type"]
                if item["detail_type"] == "":
                    item["announcement_type"] = 1
                else:
                    item["announcement_type"] = 0
                item["doc_source_url"] = "https://www.borsaitaliana.it" + temp.xpath('./td[1]/a/@href').extract()[0]
                date = temp.xpath('./td[2]/text()').extract()[0]
                item["disclosure_date"] = str(date).split("/")[-1] + "-" + str(date).split("/")[1] + "-" + str(date).split("/")[0] + " 00:00:00"
                if item["doc_source_url"] not in newstUrl and item["disclosure_date"] >= newstDate:
                    item = self.commonItem(item)
                    yield item
                else:
                    break
