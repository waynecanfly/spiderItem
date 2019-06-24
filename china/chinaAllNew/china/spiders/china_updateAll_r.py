# -*- coding: utf-8 -*-
import scrapy
import json
import datetime
import random
import time
import re
import pymysql
from china.items import ChinaIntroItem_ad
from china.items import ChinaIntroItem_cs
from china.items import ChinaIntroItem_dsn
from china.items import ChinaIntroItem_if
from china.items import ChinaIntroItem_sbi
from china.items import ChinaIntroItem_sh
from china.items import ChinaIntroItem_si
from china.items import ChinaIntroItem_stp
from china.items import ChinaIntroItem_str
from china.items import ChinaIntroItem_xsg
from china.items import ChinaIntroItem_dd
from china.items import ChinaIntroItem_shenzhen_nameChange
from china.items import ChinaIntroItem_shenzhen_status


class CodeSpider(scrapy.Spider):
    name = 'china_updateAll_r'
    allowed_domains = ['szse.cn', 'sse.com.cn']
    query_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    #shenzhen_announcement_download
    sql_ad = "select security_code,company_id,mark from company_data_source where company_id like " + "'CHN%'"
    cursor.execute(sql_ad)
    results_ad = cursor.fetchall()
    qt_type_ad = [
        {"dt": "264", "qt": "0102"}, {"dt": "265", "qt": "0105"}, {"dt": "266", "qt": "0107"}, {"dt": "267", "qt": "0109"},
        {"dt": "268", "qt": "0110"}, {"dt": "269", "qt": "0111"}, {"dt": "270", "qt": "0113"}, {"dt": "271", "qt": "0115"},
        {"dt": "272", "qt": "0117"}, {"dt": "273", "qt": "0119"}, {"dt": "274", "qt": "0121"}, {"dt": "275", "qt": "0125"},
        {"dt": "276", "qt": "0127"}, {"dt": "277", "qt": "0129"}, {"dt": "278", "qt": "0131"}, {"dt": "279", "qt": "0139"},
        {"dt": "280", "qt": "0123"}, {"dt": "281", "qt": "01239901"}, {"dt": "282", "qt": "01239910"}
    ]
    #shenzhen_continuous_supervision
    type_code_cs = ["sponsor_disc", "finance_disc"]
    code_list_cs = []
    jud_list_cs = []
    newstdate_dict_cs = {}
    #shenzhen_delisting_download
    page_dd = 1
    newstdate_dict_dd = {}
    code_list_dd = []
    #shenzhen_download_spider_new
    pattern_dsn = re.compile("\d.*?\d+")
    report_type_dsn = [{"name": "Q1", "value": "010305"}, {"name": "Q2", "value": "010303"},
                       {"name": "Q3", "value": "010307"}, {"name": "FY", "value": "010301"}]
    results_dsn = results_ad
    #shenzhen_integrity_file
    type_list_if = ["1759_cxda", "1903_detail"]
    url1_if = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID="
    url2_if = "&TABKEY=tab1&PAGENO="
    url3_if = "&random=0.17988388377929776"
    code_list_if = []
    #shenzhen_secretary_information
    url1_si = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1901&TABKEY=tab1&PAGENO="
    url2_si = "&random=0.5344944650740717"
    #shenzhen_secretary_training_record
    url1_str = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1901&TABKEY=tab2&PAGENO="
    url2_str = "&random=0.5344944650740717"
    #sensible_information
    url1_sbi = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1900&TABKEY="
    url2_sbi = "&PAGENO="
    url3_sbi = "&random=0.23734333220651216"
    table_key_list_sbi = ["tab1", "tab2"]
    #sensible_talent_pool
    url1_stp = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1900&TABKEY="
    url2_stp = "&PAGENO="
    url3_stp = "&random=0.23734333220651216"
    #xsgfjxyjc
    type_list_xsg = ["tab1", "tab2", "tab3"]
    url1_xsg = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1902&TABKEY="
    url2_xsg = "&PAGENO="
    url3_xsg = "&random=0.6829295020412363"
    #nameChange
    num_nameChange = 1
    url1_nameChange = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=SSGSGMXX&TABKEY=tab1&PAGENO="
    url2_nameChange = "&random=0.21263260097323644"
    #company_status
    table_list_status = ["1", "2"]
    url1_status = "http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1793_ssgs&TABKEY=tab"
    url2_status = "&PAGENO="
    url3_status = "&random=0.2831277777777923"

    def uniqueIDMaker(self):
        time_id = str(datetime.datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

    def start_requests(self):
        end_time = str(self.query_time).split(" ")[0]
        #delisting
        print("delisting")
        url_dd = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
        data_dd = {
            "seDate": ["2008-01-01", end_time],
            "channelCode": ["delist_disc"],
            "pageSize": "30",
            "pageNum": str(self.page_dd)
        }
        yield scrapy.Request(
            url_dd,
            method="POST",
            body=json.dumps(data_dd),
            callback=self.parse_dd,
            meta={"end_time": end_time}
        )
        # #shenzhen_integrity_file
        print("if")
        for temp_if in self.type_list_if:
            if temp_if == "1759_cxda":
                belong2type_if = "处罚与处分记录"
            else:
                belong2type_if = "中介机构处罚与处分记录"
            sql_if = "select max(punish_date),doc_source_url from shenzhen_integrity_file " \
                         "where punish_date in(select max(punish_date) as punish_date from " \
                         "shenzhen_integrity_file where belong_2_type = %s) and belong_2_type = %s "
            self.cursor.execute(sql_if, [belong2type_if, belong2type_if])
            results_if = self.cursor.fetchall()
            if results_if[0][0]:
                newstdate_if = str(results_if[0][0])
                newsturl_if = [i[1] for i in results_if]
            else:
                newstdate_if = "0"
                newsturl_if = []
            page_if = 1
            flag = 0
            url_if = self.url1_if + temp_if + self.url2_if + str(page_if) + self.url3_if
            yield scrapy.Request(
                url_if,
                callback=self.parse_if,
                meta={"belong_2_type": temp_if, "page": page_if,
                      "newstdate": newstdate_if, "newsturl": newsturl_if, "flag": flag}
            )
        #shenzhen_secretary_information
        print("si")
        page_si = 1
        url_si = self.url1_si + str(page_si) + self.url2_si
        yield scrapy.Request(
            url_si,
            callback=self.parse_si,
            meta={"page": page_si}
        )
        #secretary_training_record
        print("str")
        page_str = 1
        url_str = self.url1_str + str(page_str) + self.url2_str
        yield scrapy.Request(
            url_str,
            callback=self.parse_str,
            meta={"page": page_str}
        )
        #sensible_information
        print("sbi")
        for temp_sbi in self.table_key_list_sbi:
            page_sbi = 1
            url_sbi = self.url1_sbi + temp_sbi + self.url2_sbi + str(page_sbi) + self.url3_sbi
            sensible_type = temp_sbi
            yield scrapy.Request(
                url_sbi,
                callback=self.parse_sbi,
                meta={"sensible_type": sensible_type, "page": page_sbi}
            )
        #sensible_talent_pool
        print("stp")
        page_stp = 1
        url_stp = self.url1_stp + "tab3" + self.url2_stp + str(page_stp) + self.url3_stp
        yield scrapy.Request(
            url_stp,
            callback=self.parse_stp,
            meta={"page": page_stp}
        )
        #xsgfjxyjc
        print("xsgfjxyjc")
        for temp_xsg in self.type_list_xsg:
            page_xsg = 1
            url_xsg = self.url1_xsg + temp_xsg + self.url2_xsg + str(page_xsg) + self.url3_xsg
            yield scrapy.Request(
                url_xsg,
                callback=self.parse_xsg,
                meta={"page": page_xsg, "belong_2_type": temp_xsg}
            )
        #nameChange
        print("nc")
        url_nameChange = self.url1_nameChange + str(self.num_nameChange) + self.url2_nameChange
        yield scrapy.Request(url_nameChange,  callback=self.parse_nameChange)
        #company_status
        print("status")
        for temp_status in self.table_list_status:
            num_status = 1
            if temp_status == "1":
                status = "pause"
            else:
                status = "delisting"
            link_status = self.url1_status + temp_status + self.url2_status
            url_status = link_status + str(num_status) + self.url3_status
            yield scrapy.Request(url_status,  callback=self.parse_status, meta={"status": status, "link": link_status, "num": num_status})
        #shenzhen_continuous_supervision
        print("cs")
        for temp_cs in self.type_code_cs:
            page_cs = 1
            url_cs = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
            data_cs = {
                "seDate": ["2008-01-01", end_time],
                "channelCode": [temp_cs],
                "pageSize": "30",
                "pageNum": str(page_cs)
            }
            yield scrapy.Request(
                url_cs,
                method="POST",
                body=json.dumps(data_cs),
                callback=self.parse_cs,
                meta={"belong_2_type": temp_cs, "page": page_cs, "end_time": end_time}
            )
        # annoucement_download
        print("ad")
        for temp in self.qt_type_ad:
            flag_ad = 0
            page_ad = 1
            dt_ad = temp["dt"]
            qt_ad = temp["qt"]
            sql_select_ad = "select disclosure_date,doc_source_url from non_financial_statement_index where disclosure_date " \
                             "in(select max(disclosure_date) as disclosure_date from non_financial_statement_index " \
                             "where exchange_market_code = 'SZSE' and Non_financial_announcement_detail_type=%s and is_deleted ='0')" \
                             "and exchange_market_code = 'SZSE' and Non_financial_announcement_detail_type=%s and is_deleted ='0'"
            self.cursor.execute(sql_select_ad, [dt_ad, dt_ad])
            result_ad = self.cursor.fetchall()
            if len(result_ad) != 0:
                newstdate_ad = result_ad[0][0]
                newsturl_ad = [str(i[1]) for i in result_ad]
            else:
                newstdate_ad = "0"
                newsturl_ad = []
            url_ad = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
            data_ad = {
                "seDate": ["2007-01-01", end_time],
                "channelCode": ["listedNotice_disc"],
                "bigCategoryId": [qt_ad],
                "pageSize": "30",
                "pageNum": "1"
            }
            yield scrapy.Request(
                url_ad,
                method="POST",
                body=json.dumps(data_ad),
                callback=self.parse_ad, meta={
                    "newstdate_ad": newstdate_ad, "newsturl_ad": newsturl_ad,
                    "dt": dt_ad, "qt": qt_ad, "page": page_ad, "end_time": end_time, "flag_ad": flag_ad
                })

        # shenzhen_download_spider_new 另外采用新的更新策略
        # print("dsn")
        # for each_dsn in self.report_type_dsn:
        #     flag_dsn = 0
        #     page_dsn = 1
        #     sql_select_dsn = "select disclosure_date,doc_source_url from financial_statement_index where disclosure_date " \
        #                      "in(select max(disclosure_date) as disclosure_date from financial_statement_index " \
        #                      "where exchange_market_code = 'SZSE' and doc_type='pdf' and financial_statement_season_type_code = %s)" \
        #                      "and exchange_market_code = 'SZSE' and doc_type='pdf' and financial_statement_season_type_code = %s"
        #     self.cursor.execute(sql_select_dsn, [each_dsn["name"], each_dsn["name"]])
        #     results_dsn = self.cursor.fetchall()
        #     newstdate = results_dsn[0][0]
        #     newsturl = [str(i[1]) for i in results_dsn]
        #     financial_statement_season_type_code_dsn = each_dsn["name"]
        #     url_dsn = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
        #     data_dsn = {
        #         "seDate": ["2008-01-01", end_time],
        #         "channelCode": ["fixed_disc"],
        #         "bigCategoryId": [each_dsn["value"]],
        #         "pageSize": "30",
        #         "pageNum": "1"
        #     }
        #     yield scrapy.Request(
        #         url_dsn,
        #         method="POST",
        #         body=json.dumps(data_dsn),
        #         callback=self.parse_dsn, meta={
        #             "financial_statement_season_type_code": financial_statement_season_type_code_dsn,
        #             "newstdate": newstdate, "newsturl": newsturl,
        #             "page": page_dsn,
        #             "end_time": end_time,
        #             "qt": each_dsn["value"],
        #             "flag_dsn": flag_dsn
        #         })

    def parse_status(self, response):
        num = response.meta["num"]
        status = response.meta["status"]
        link = response.meta["link"]
        if status == "delisting":
            dataList = json.loads(response.body)[1]["data"]
            for temp_status in dataList:
                item = ChinaIntroItem_shenzhen_status()
                item["ipo_date"] = temp_status["ssrq"] + " 00:00:00"
                item["code"] = temp_status["zqdm"]
                item["end_date"] = temp_status["zzrq"] + " 00:00:00"
                item["gmt_update"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["doc_source_url"] = None
                item["status"] = status
                item["exchange_market_code"] = "SZSE"
                yield item
        else:
            dataList = json.loads(response.body)[0]["data"]
            for temp_status in dataList:
                item = ChinaIntroItem_shenzhen_status()
                item["ipo_date"] = temp_status["ssrq"] + " 00:00:00"
                item["code"] = temp_status["zqdm"]
                item["end_date"] = temp_status["ztrq"] + " 00:00:00"
                item["gmt_update"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["doc_source_url"] = None
                item["status"] = status
                item["exchange_market_code"] = "SZSE"
                yield item
        if len(dataList) != 0:
            num += 1
            url = link + str(num) + self.url3_status
            yield scrapy.Request(url,  callback=self.parse_status, meta={"status": status, "link": link, "num": num})

    def parse_nameChange(self, response):
        dataList = json.loads(response.body)[0]["data"]
        if len(dataList) != 0:
            for temp in dataList:
                item = ChinaIntroItem_shenzhen_nameChange()
                item["end_date"] = temp["bgrq"] + " 00:00:00"
                item["code"] = temp["zqdm"]
                item["fomer_name"] = temp["bgqgsqc"]
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["doc_source_url"] = None
                yield item
            self.num_nameChange += 1
            url = self.url1_nameChange + str(self.num_nameChange) + self.url2_nameChange
            yield scrapy.Request(url,  callback=self.parse_nameChange)

    def parse_xsg(self, response):
        belong_2_type = response.meta["belong_2_type"]
        page = response.meta["page"]
        if belong_2_type == "tab1":
            data_list = json.loads(response.body)[0]["data"]
            for temp in data_list:
                item = ChinaIntroItem_xsg()
                item["security_code"] = temp["zqdm"]
                item["exchange_market_code"] = "SZSE"
                item["release_sale_date"] = temp["jxrq"]
                item["release_sale_shareholder_num"] = temp["gdrs"]
                item["release_sale_stock_num"] = temp["jxsl"]
                item["release_sale_stock_rate"] = temp["jxbl"]
                item["shareholder_name"] = None
                item["managed_member_name"] = None
                item["disclosure_date"] = None
                item["bcpljcgf_num"] = None
                item["bcpljcgf_rate"] = None
                item["tgdzjyjc_num"] = None
                item["tgdzjyjc_rate"] = None
                item["jchrcygf_num"] = None
                item["jchrcygf_rate"] = None
                item["belong_2_type"] = "tab1"
                item["doc_source_url"] = None
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["spiderName"] = "shenzhen_xsgfjxyjc"
                yield item
        elif belong_2_type == "tab2":
            data_list = json.loads(response.body)[1]["data"]
            for temp in data_list:
                item = ChinaIntroItem_xsg()
                item["security_code"] = temp["zqdm"]
                item["exchange_market_code"] = "SZSE"
                item["release_sale_date"] = temp["jxrq"]
                item["release_sale_shareholder_num"] = None
                item["release_sale_stock_num"] = temp["jxsl"]
                item["release_sale_stock_rate"] = temp["jxbl"]
                item["shareholder_name"] = temp["gdmc"]
                item["managed_member_name"] = temp["hyjc"]
                item["disclosure_date"] = None
                item["bcpljcgf_num"] = None
                item["bcpljcgf_rate"] = None
                item["tgdzjyjc_num"] = None
                item["tgdzjyjc_rate"] = None
                item["jchrcygf_num"] = None
                item["jchrcygf_rate"] = None
                item["belong_2_type"] = "tab2"
                item["doc_source_url"] = None
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["spiderName"] = "shenzhen_xsgfjxyjc"
                yield item
        else:
            data_list = json.loads(response.body)[2]["data"]
            for temp in data_list:
                item = ChinaIntroItem_xsg()
                item["security_code"] = temp["zqdm"]
                item["exchange_market_code"] = "SZSE"
                item["release_sale_date"] = None
                item["release_sale_shareholder_num"] = None
                item["release_sale_stock_num"] = None
                item["release_sale_stock_rate"] = None
                item["shareholder_name"] = temp["gdmc"]
                item["managed_member_name"] = None
                item["disclosure_date"] = None
                item["bcpljcgf_num"] = temp["jcsl"]
                item["bcpljcgf_rate"] = temp["jcbl"]
                item["tgdzjyjc_num"] = temp["dzjcsl"]
                item["tgdzjyjc_rate"] = temp["dzjcbl"]
                item["jchrcygf_num"] = temp["cgsl"]
                item["jchrcygf_rate"] = temp["cgbl"]
                item["belong_2_type"] = "tab3"
                item["doc_source_url"] = None
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["spiderName"] = "shenzhen_xsgfjxyjc"
                yield item
        if len(data_list) != 0:
            page += 1
            url = self.url1_xsg + belong_2_type + self.url2_xsg + str(page) + self.url3_xsg
            yield scrapy.Request(
                url,
                callback=self.parse_xsg,
                meta={"page": page, "belong_2_type": belong_2_type}
            )

    def parse_stp(self, response):
        page = response.meta["page"]
        data_list = json.loads(response.body)[2]["data"]
        for temp in data_list:
            item = ChinaIntroItem_stp()
            xyxm = re.search(">(.*?)</a>", str(temp["xyxm"]))
            item["college_name"] = xyxm.group(1)
            item["exchange_market_code"] = "SZSE"
            item["training_date"] = temp["pxjsrq"]
            item["training_num"] = temp["pxqs"]
            item["gender"] = temp["xb"]
            item["age"] = temp["nl"]
            item["education"] = temp["xl"]
            item["discipline"] = temp["zy"]
            item["job_title"] = temp["zc"]
            item["doc_source_url"] = None
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "zx"
            item["spiderName"] = "shenzhen_sensible_talent_pool"
            yield item
        if len(data_list) != 0:
            page += 1
            url = self.url1_stp + "tab3" + self.url2_stp + str(page) + self.url3_stp
            yield scrapy.Request(
                url,
                callback=self.parse_stp,
                meta={"page": page}
            )

    def parse_sbi(self, response):
        page = response.meta["page"]
        sensible_type = response.meta["sensible_type"]
        if sensible_type == "tab1":
            data_list = json.loads(response.body)[0]["data"]
            for temp in data_list:
                item = ChinaIntroItem_sbi()
                item["file_name"] = temp["dldsxm"]
                item["exchange_market_code"] = "SZSE"
                item["security_code"] = temp["gsdm"]
                item["disclosure_date"] = temp["gsqj"]
                item["doc_type"] = "拟聘"
                item["doc_source_url"] = None
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["spiderName"] = "shenzhen_sensible_information"
                yield item
        else:
            data_list = json.loads(response.body)[1]["data"]
            for temp in data_list:
                item = ChinaIntroItem_sbi()
                item["file_name"] = temp["dldsxm"]
                item["exchange_market_code"] = "SZSE"
                item["security_code"] = temp["gsdm"]
                item["doc_type"] = "在任"
                item["disclosure_date"] = None
                item["doc_source_url"] = None
                item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                item["user_create"] = "zx"
                item["spiderName"] = "shenzhen_sensible_information"
                yield item
        if len(data_list) != 0:
            page += 1
            url = self.url1_sbi + sensible_type + self.url2_sbi + str(page) + self.url3_sbi
            yield scrapy.Request(
                url,
                callback=self.parse_sbi,
                meta={"sensible_type": sensible_type, "page": page}
            )

    def parse_str(self, response):
        page = response.meta["page"]
        data_list = json.loads(response.body)[1]["data"]
        for temp in data_list:
            item = ChinaIntroItem_str()
            item["secretary_name"] = temp["xm"]
            item["exchange_market_code"] = "SZSE"
            item["start_date"] = temp["hddmzgsj"]
            item["recent_training_date"] = temp["zjycpxwcrq"]
            item["gender"] = temp["xb"]
            item["education"] = temp["xl"]
            item["doc_source_url"] = None
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "zx"
            item["spiderName"] = "shenzhen_secretary_training_record"
            yield item
        if len(data_list) != 0:
            page += 1
            url = self.url1_str + str(page) + self.url2_str
            yield scrapy.Request(
                url,
                callback=self.parse_str,
                meta={"page": page}
            )

    def parse_si(self, response):
        page = response.meta["page"]
        data_list = json.loads(response.body)[0]["data"]
        for temp in data_list:
            item = ChinaIntroItem_si()
            item["secretary_name"] = temp["xm"]
            item["security_code"] = temp["gsdm"]
            item["exchange_market_code"] = "SZSE"
            item["start_date"] = temp["hddmzgsj"]
            item["recent_training_date"] = temp["zjycpxwcrq"]
            item["gender"] = temp["xb"]
            item["job_title"] = temp["rzqk"]
            item["doc_source_url"] = None
            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            item["user_create"] = "zx"
            item["spiderName"] = "shenzhen_secretary_information"
            yield item
        if len(data_list) != 0:
            page += 1
            url = self.url1_si + str(page) + self.url2_si
            yield scrapy.Request(
                url,
                callback=self.parse_si,
                meta={"page": page}
            )

    def parse_if(self, response):
        flag = response.meta["flag"]
        newsturl = response.meta["newsturl"]
        newstdate = response.meta["newstdate"]
        page = response.meta["page"]
        belong_2_type = response.meta["belong_2_type"]
        data_list = json.loads(response.body)[0]["data"]
        if len(data_list) != 0:
            if belong_2_type == "1759_cxda":
                for temp in data_list:
                    item = ChinaIntroItem_if()
                    title = temp["bt"]
                    link = temp["ck"]
                    data = re.search("encode-open='(.*?)'>", link)
                    item["punish_date"] = temp["xx_fwrq"]
                    date_if = str(item["punish_date"]).split("-")[0]
                    item["doc_source_url"] = "http://www.szse.cn" + data.group(1)
                    if item["punish_date"] >= newstdate and item["doc_source_url"] not in newsturl:
                        item["litigant"] = temp["dsr"]
                        item["punish_type"] = temp["cflb"]
                        item["exchange_market_code"] = "SZSE"
                        item["security_code"] = temp["xx_gsdm"]
                        sql = "select company_id from company_data_source where security_code = %s"
                        self.cursor.execute(sql, item["security_code"])
                        result = self.cursor.fetchone()
                        if result:
                            unique_id = self.uniqueIDMaker()
                            item["report_id"] = result[0] + unique_id
                            item["doc_local_path"] = "/volume3/homes3/China/" + date_if + "/" + item[
                                "report_id"] + ".pdf"
                            item["is_doc_url_direct"] = 1
                            item["doc_type"] = "pdf"
                            item["is_downloaded"] = 1
                            item["language_written_code"] = "zh-simple"
                            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                            item["user_create"] = "zx"
                            item["file_name"] = title
                            item["agency_name"] = None
                            item["agency_type"] = None
                            item["belong_2_type"] = "处罚与处分记录"
                            item["spiderName"] = "shenzhen_integrity_file"
                            yield item
                    else:
                        flag += 1
                        # print(newstdate, newsturl, item["doc_source_url"], item["punish_date"], "=" * 100)
                        break
            else:
                for temp in data_list:
                    item = ChinaIntroItem_if()
                    title = temp["zj_bt"]
                    link = temp["ck"]
                    data = re.search("encode-open='(.*?)'>", link)
                    item["punish_date"] = temp["zj_fwrq"]
                    item["doc_source_url"] = "http://www.szse.cn" + data.group(1)
                    date_if = str(item["punish_date"]).split("-")[0]
                    if item["punish_date"] >= newstdate and item["doc_source_url"] not in newsturl:
                        item["litigant"] = temp["zj_dsr"]
                        item["punish_type"] = temp["zj_cflb"]
                        item["exchange_market_code"] = "SZSE"
                        item["security_code"] = temp["zj_gsdm"]
                        sql = "select company_id from company_data_source where security_code = %s"
                        self.cursor.execute(sql, item["security_code"])
                        result = self.cursor.fetchone()
                        if result:
                            unique_id = self.uniqueIDMaker()
                            item["report_id"] = result[0] + unique_id
                            item["doc_local_path"] = "/volume3/homes3/China/" + date_if + "/" + item[
                                "report_id"] + ".pdf"
                            item["is_doc_url_direct"] = 1
                            item["doc_type"] = "pdf"
                            item["is_downloaded"] = 1
                            item["language_written_code"] = "zh-simple"
                            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                            item["user_create"] = "zx"
                            item["file_name"] = title
                            item["agency_name"] = temp["zj_zjrm"]
                            item["agency_type"] = temp["zj_cfdx"]
                            item["belong_2_type"] = "中介机构处罚与处分记录"
                            item["spiderName"] = "shenzhen_integrity_file"
                            yield item
                    else:
                        flag += 1
                        # print(newstdate, newsturl, item["doc_source_url"], item["punish_date"], "=" * 100)
                        break
            if flag == 0:
                page += 1
                url = self.url1_if + belong_2_type + self.url2_if + str(page) + self.url3_if
                yield scrapy.Request(
                    url,
                    callback=self.parse_if,
                    meta={"belong_2_type": belong_2_type, "page": page,
                          "newstdate": newstdate, "newsturl": newsturl, "flag": flag}
                )

    def parse_ad(self, response):
        flag_ad = response.meta["flag_ad"]
        page = response.meta["page"]
        qt = response.meta["qt"]
        end_time = response.meta["end_time"]
        newstdate_ad = response.meta["newstdate_ad"]
        newsturl_ad = response.meta["newsturl_ad"]
        dt = response.meta["dt"]
        data_list = json.loads(response.body)["data"]
        for temp in data_list:
            item = ChinaIntroItem_ad()
            item["disclosure_date"] = temp["publishTime"]
            item["doc_source_url"] = "http://disc.static.szse.cn/download" + temp["attachPath"]
            # if item["disclosure_date"] > str(newst_ad):
            if item["doc_source_url"] not in newsturl_ad and item["disclosure_date"] >= str(newstdate_ad):
                security_code = temp["secCode"][0]
                title = temp["title"]
                item["exchange_market_code"] = "SZSE"
                sql = "select code from company where security_code=%s and exchange_market_code= 'SZSE'"
                self.cursor.execute(sql, security_code)
                result = self.cursor.fetchone()
                if result:
                    item["company_code"] = result[0]
                    item["fiscal_year"] = str(item["disclosure_date"]).split("-")[0]
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["doc_local_path"] = "/volume3/homes3/China/" + item["fiscal_year"] + "/" + item["report_id"] + ".pdf"
                    item["country_code"] = "CHN"
                    item["is_doc_url_direct"] = 1
                    item["doc_type"] = "pdf"
                    item["is_downloaded"] = 1
                    item["language_written_code"] = "zh-simple"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["doc_downloaded_timestamp"] = item["gmt_create"]
                    item["user_create"] = "zx"
                    item["file_name"] = title
                    item["spiderName"] = "shenzhen_announcement_download"
                    item["detail_type"] = dt
                    yield item
            else:
                flag_ad += 1
                break
        if flag_ad == 0:
            if len(data_list) > 0:
                page += 1
                url_ad = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
                data_ad = {
                    "seDate": ["2007-01-01", end_time],
                    "channelCode": ["listedNotice_disc"],
                    "bigCategoryId": [qt],
                    "pageSize": "30",
                    "pageNum": str(page)
                }
                yield scrapy.Request(
                    url_ad,
                    method="POST",
                    body=json.dumps(data_ad),
                    callback=self.parse_ad, meta={
                        "newstdate_ad": newstdate_ad, "newsturl_ad": newsturl_ad,
                        "dt": dt, "qt": qt, "page": page, "end_time": end_time, "flag_ad": flag_ad
                    })

    def parse_cs(self, response):
        end_time = response.meta["end_time"]
        page = response.meta["page"]
        belong_2_type = response.meta["belong_2_type"]
        data_list = json.loads(response.body)["data"]
        if len(data_list) != 0:
            for temp in data_list:
                item = ChinaIntroItem_cs()
                item["disclosure_date"] = temp["publishTime"]
                date_cs = str(item["disclosure_date"]).split("-")[0]
                title = temp["title"]
                item["doc_source_url"] = "http://disc.static.szse.cn/download" + temp["attachPath"]
                item["exchange_market_code"] = "SZSE"
                item["security_code"] = temp["secCode"][0]
                sql = "select company_id from company_data_source where security_code = %s"
                self.cursor.execute(sql, item["security_code"])
                result = self.cursor.fetchone()
                if result:
                    if belong_2_type == "finance_disc":
                        item["belong_2_type"] = "财务顾问持续督导意见"
                    else:
                        item["belong_2_type"] = "保荐机构持续督导意见"
                    sql_select = "select max(disclosure_date),doc_source_url from shenzhen_continuous_supervision " \
                                 "where disclosure_date in(select max(disclosure_date) as disclosure_date from " \
                                 "shenzhen_continuous_supervision where security_code = %s and belong_2_type = %s) " \
                                 "and security_code = %s and belong_2_type = %s "
                    self.cursor.execute(sql_select, [item["security_code"], item["belong_2_type"],
                                                     item["security_code"], item["belong_2_type"]])
                    result_cs = self.cursor.fetchall()
                    if result_cs[0][0]:
                        newstdate = str(result_cs[0][0])
                        newsturl = [i[1] for i in result_cs]
                        if item["doc_source_url"] not in newsturl and item["disclosure_date"] >= newstdate:
                            unique_id = self.uniqueIDMaker()
                            item["report_id"] = result[0] + unique_id
                            item["doc_local_path"] = "/volume3/homes3/China/" + date_cs + "/" + item["report_id"] + ".pdf"
                            item["is_doc_url_direct"] = 1
                            item["doc_type"] = "pdf"
                            item["is_downloaded"] = 1
                            item["language_written_code"] = "zh-simple"
                            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                            item["user_create"] = "zx"
                            item["file_name"] = title
                            item["spiderName"] = "shenzhen_continuous_supervision"
                            yield item
                        else:
                            # print(newstdate, newsturl, item["doc_source_url"], item["disclosure_date"], "=" * 100)
                            break
                    else:
                        unique_id = self.uniqueIDMaker()
                        item["report_id"] = result[0] + unique_id
                        item["doc_local_path"] = "/volume3/homes3/China/" + date_cs + "/" + item["report_id"] + ".pdf"
                        item["is_doc_url_direct"] = 1
                        item["doc_type"] = "pdf"
                        item["is_downloaded"] = 1
                        item["language_written_code"] = "zh-simple"
                        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        item["user_create"] = "zx"
                        item["file_name"] = title
                        item["spiderName"] = "shenzhen_continuous_supervision"
                        yield item
            page += 1
            url = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
            data = {
                "seDate": ["2008-01-01", end_time],
                "channelCode": [belong_2_type],
                "pageSize": "30",
                "pageNum": str(page)
            }
            yield scrapy.Request(
                url,
                method="POST",
                body=json.dumps(data),
                callback=self.parse_cs,
                meta={"belong_2_type": belong_2_type, "page": page, "end_time": end_time}
            )

    def parse_dd(self, response):
        end_time = response.meta["end_time"]
        data_list = json.loads(response.body)["data"]
        if len(data_list) != 0:
            for temp in data_list:
                item = ChinaIntroItem_dd()
                item["disclosure_date"] = temp["publishTime"]
                date_dd = str(item["disclosure_date"]).split("-")[0]
                title = temp["title"]
                item["doc_source_url"] = "http://disc.static.szse.cn/download" + temp["attachPath"]
                item["exchange_market_code"] = "SZSE"
                item["security_code"] = temp["secCode"][0]
                sql = "select company_id from company_data_source where security_code = %s"
                self.cursor.execute(sql, item["security_code"])
                result_dd = self.cursor.fetchone()
                if result_dd:
                    sql_select = "select max(disclosure_date),doc_source_url from shenzhen_delisting_announcement " \
                                 "where disclosure_date in(select max(disclosure_date) as disclosure_date from " \
                                 "shenzhen_delisting_announcement where security_code = %s) and security_code = %s"
                    self.cursor.execute(sql_select, [item["security_code"],item["security_code"]])
                    result = self.cursor.fetchall()
                    if result[0][0]:
                        newstdate = str(result[0][0])
                        newsturl = [i[1] for i in result]
                        if item["doc_source_url"] not in newsturl and item["disclosure_date"] >= newstdate:
                            unique_id = self.uniqueIDMaker()
                            item["report_id"] = result_dd[0] + unique_id
                            item["doc_local_path"] = "/volume3/homes3/China/" + date_dd + "/" + item["report_id"] + ".pdf"
                            item["is_doc_url_direct"] = 1
                            item["doc_type"] = "pdf"
                            item["is_downloaded"] = 1
                            item["language_written_code"] = "zh-simple"
                            item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                            item["user_create"] = "zx"
                            item["file_name"] = title
                            item["spiderName"] = "shenzhen_delisting_download"
                            yield item
                        else:
                            # print(newstdate, newsturl, item["doc_source_url"], item["disclosure_date"], "=" * 100)
                            break
                    else:
                        unique_id = self.uniqueIDMaker()
                        item["report_id"] = result_dd[0] + unique_id
                        item["doc_local_path"] = "/volume3/homes3/China/" + date_dd + "/" + item["report_id"] + ".pdf"
                        item["is_doc_url_direct"] = 1
                        item["doc_type"] = "pdf"
                        item["is_downloaded"] = 1
                        item["language_written_code"] = "zh-simple"
                        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        item["user_create"] = "zx"
                        item["file_name"] = title
                        item["spiderName"] = "shenzhen_delisting_download"
                        yield item
            self.page_dd += 1
            url = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
            data = {
                "seDate": ["2008-01-01", end_time],
                "channelCode": ["delist_disc"],
                "pageSize": "30",
                "pageNum": str(self.page_dd)
            }
            yield scrapy.Request(
                url,
                method="POST",
                body=json.dumps(data),
                callback=self.parse_dd,
                meta={"end_time": end_time}
            )

    def parse_dsn(self, response):
        flag_dsn = response.meta["flag_dsn"]
        qt = response.meta["qt"]
        end_time = response.meta["end_time"]
        fst = response.meta["financial_statement_season_type_code"]
        page = response.meta["page"]
        newstdate = response.meta["newstdate"]
        newsturl = response.meta["newsturl"]
        data_list = json.loads(response.body)["data"]
        for temp in data_list:
            item = ChinaIntroItem_dsn()
            item["disclosure_date"] = temp["publishTime"]
            item["doc_source_url"] = "http://disc.static.szse.cn/download" + temp["attachPath"]
            if item["disclosure_date"] >= str(newstdate) and item["doc_source_url"] not in newsturl:
                title = temp["title"]
                security_code = temp["secCode"][0]
                item["exchange_market_code"] = "SZSE"
                sql = "select code from company where security_code=%s and exchange_market_code= 'SZSE'"
                self.cursor.execute(sql, security_code)
                result = self.cursor.fetchone()
                if result:
                    item["company_code"] = result[0]
                    item["financial_statement_season_type_code"] = fst
                    try:
                        item["fiscal_year"] = self.pattern_dsn.search(str(title)).group()
                        fiscal_year = item["fiscal_year"]
                    except:
                        item["fiscal_year"] = None
                        fiscal_year = "0000"
                    item["report_id"] = item["company_code"] + self.uniqueIDMaker()
                    item["doc_local_path"] = "/volume3/homes3/China/" + str(fiscal_year) + "/" + item["report_id"] + ".pdf"
                    item["country_code"] = "CHN"
                    item["is_doc_url_direct"] = 1
                    item["financial_reporting_standard_code"] = "CAS"
                    item["doc_type"] = "pdf"
                    item["is_downloaded"] = 1
                    item["currency_code"] = "CNY"
                    item["language_written_code"] = "zh-simple"
                    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                    item["doc_downloaded_timestamp"] = item["gmt_create"]
                    item["user_create"] = "zx"
                    item["file_name"] = title
                    item["spiderName"] = "shenzhen_download_spider_new"
                    item["announcement_type"] = 1
                    yield item
            else:
                flag_dsn += 1
                break
        if flag_dsn == 0:
            if len(data_list) > 0:
                page += 1
                url_dsn = "http://www.szse.cn/api/disc/announcement/annList?random=0.8865787581291689"
                data_dsn = {
                    "seDate": ["2008-01-01", end_time],
                    "channelCode": ["fixed_disc"],
                    "bigCategoryId": [qt],
                    "pageSize": "30",
                    "pageNum": str(page)
                }
                yield scrapy.Request(
                    url_dsn,
                    method="POST",
                    body=json.dumps(data_dsn),
                    callback=self.parse_dsn, meta={
                        "financial_statement_season_type_code": fst,
                        "newstdate": newstdate, "newsturl": newsturl,
                        "page": page,
                        "end_time": end_time,
                        "qt": qt,
                        "flag_dsn": flag_dsn
                    })