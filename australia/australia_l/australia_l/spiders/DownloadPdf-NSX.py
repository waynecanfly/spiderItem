# -*- coding: utf-8 -*-
import scrapy
import re
import time
import pymysql
import random
from datetime import datetime
from australia_l.items import AustraliacompanyupdateNSXItem


class AstraliaSpiderNSX(scrapy.Spider):
    name = 'DownloadPdf-NSX'
    allowed_domains = ['nsx.com.au']
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code, company_id from company_data_source where company_id like 'AUS%' and " \
          "download_link = 'https://www.nsx.com.au/marketdata/directory/'"
    cursor.execute(sql)
    results = cursor.fetchall()
    FY_list = ["annual report"]
    Q_list = ["quarterly report"]
    Q2_list = ["half yearly report"]

    def uniqueIDMaker(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        url1 = "https://www.nsx.com.au/marketdata/company-directory/announcements/"
        url2 = "/"
        for temp in self.results:
            NSX_code = temp[0]
            company_id = temp[1]
            sql_select = "select disclosure_date,doc_source_url from financial_statement_index where disclosure_date " \
                         "in(select max(disclosure_date) as disclosure_date from financial_statement_index where " \
                         "country_code = 'AUS' and exchange_market_code ='NSX' and company_code = %s) and " \
                         "country_code = 'AUS' and exchange_market_code ='NSX' and company_code = %s"
            self.cursor.execute(sql_select, [company_id, company_id])
            results = self.cursor.fetchall()
            if len(results) != 0:
                newstdate = str(results[0][0])
                newsturl = [i[1] for i in results]
            else:
                newstdate = "0"
                newsturl = []
            sql_select_non = "select disclosure_date,doc_source_url from non_financial_statement_index where disclosure_date " \
                         "in(select max(disclosure_date) as disclosure_date from non_financial_statement_index where " \
                         "country_code = 'AUS' and exchange_market_code ='NSX' and company_code = %s) and " \
                         "country_code = 'AUS' and exchange_market_code ='NSX' and company_code = %s"
            self.cursor.execute(sql_select_non, [company_id, company_id])
            results_non = self.cursor.fetchall()
            if len(results_non) != 0:
                newstdate_non = str(results_non[0][0])
                newsturl_non = [i[1] for i in results_non]
            else:
                newstdate_non = "0"
                newsturl_non = []
            if newstdate <= newstdate_non:
                newstdate = newstdate_non
                newsturl = newsturl_non
            url = url1 + str(NSX_code) + url2
            yield scrapy.Request(url, callback=self.parse, meta={"company_id": company_id, "newstdate": newstdate, "newsturl": newsturl})

    def parse(self, response):
        newstdate = response.meta["newstdate"]
        newsturl = response.meta["newsturl"]
        pattern = re.compile("(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})")
        company_id = response.meta["company_id"]
        link_list = response.xpath('//table[@id="myTable"]//tbody/tr')
        for temp in link_list:
            item = AustraliacompanyupdateNSXItem()
            try:
                link = temp.xpath('./td[2]/a/@href').extract()[0]
                item["doc_source_url"] = "http://www.nsx.com.au" + link
                item["original_title"] = str(temp.xpath('./td[2]/a/text()').extract()[0]).replace("\n", "").replace("\t", "")
                get_time = temp.xpath('./td[3]/span/text()').extract()[0]
                standard_time = pattern.search(get_time)
                item["disclosure_date"] = standard_time.group(1) + "-" + standard_time.group(2) + "-" + standard_time.group(3) + " " + standard_time.group(4) + ":" + standard_time.group(5) + ":" + standard_time.group(6)
                if item["doc_source_url"] not in newsturl and item["disclosure_date"] >= newstdate:
                    item["fiscal_year"] = standard_time.group(1)
                    title = temp.xpath('./td[2]/a/text()').extract()[0]
                    title_lower = str(title).lower()
                    item["country_code"] = "AUS"
                    item["exchange_market_code"] = "NSX"
                    item["company_code"] = company_id
                    item["financial_reporting_standard_code"] = "AASB"
                    item["language_written_code"] = "en"
                    item["doc_type"] = "pdf"
                    item["is_doc_url_direct"] = 1
                    item["doc_downloaded_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["is_downloaded"] = 1
                    item["currency_code"] = "AUD"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["user_create"] = "zx"
                    if any(i in title_lower for i in self.Q2_list):
                        item["financial_statement_season_type_code"] = "Q2"
                        item["report_id"] = company_id + self.uniqueIDMaker()
                        item["announcement_type"] = 1
                    elif any(i in title_lower for i in self.Q_list):
                        item["financial_statement_season_type_code"] = "Q"
                        item["report_id"] = company_id + self.uniqueIDMaker()
                        item["announcement_type"] = 1
                    elif any(i in title_lower for i in self.FY_list):
                        item["financial_statement_season_type_code"] = "FY"
                        item["report_id"] = company_id + self.uniqueIDMaker()
                        item["announcement_type"] = 1
                    else:
                        item["report_id"] = company_id + self.uniqueIDMaker()
                        item["announcement_type"] = 0
                    item["doc_local_path"] = "/volume3/homes3/Australia/" + item["report_id"] + ".pdf"
                    item["pdf_name"] = item["report_id"]
                    yield item
                else:
                    break
            except:
                print("%s无公告" % company_id)
