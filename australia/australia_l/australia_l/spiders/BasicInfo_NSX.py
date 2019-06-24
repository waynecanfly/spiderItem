# -*- coding: utf-8 -*-
import scrapy
import pymysql
import time
import re


class BasicInfoNsx(scrapy.Spider):
    name = "BasicInfo_NSX"
    allowed_domains = ['nsx.com.au']
    num = 0
    exclude_list = ["company_id", "Security_code", "ISIN", "Security_Type", "Security_Status", "gmt_create"]
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code, company_id from company_data_source where company_id like 'AUS%' and " \
          "download_link = 'https://www.nsx.com.au/marketdata/directory/' and mark = 0"
    # sql = "select security_code, company_id from company_data_source where company_id like 'AUS%' and " \
    #       "download_link = 'https://www.nsx.com.au/marketdata/directory/'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def CompanyAndDataSource(self, item, param):
        try:
            ISIN = item["ISIN"]
            Security_Type = item["Security_Type"]
            status = item["Security_Status"]
            company_name = str(item["NSX_Listed_Securities"]).split("-")[1].strip()
            listing_date = str(item["Listing_Date"]).split(",")[-1].strip().split(" ")
            listing_date = listing_date[-1] + "-" + \
                           listing_date[1].replace("January", "01").replace("February", "02").replace("March", "03").replace("April", "04").replace(
                                                   "May", "05").replace("June", "06").replace("July", "07").replace("August", "08").replace("September",
                                                    "09").replace("October", "10").replace("November", "11").replace("December", "12") + "-" + listing_date[0] + " 00:00:00"
            web = item["Web"]
        except:
            ISIN = None
            status = None
            company_name = None
            listing_date = None
            web = None
            Security_Type = None
        spider_name = "BasicInfo_NSX"
        is_batch = 1
        download_link = "https://www.nsx.com.au/marketdata/directory/"
        parameter_data_source = [
            company_name,
            download_link,
            spider_name,
            is_batch,
            item["gmt_create"],
            "zx",
            1,
            item["company_id"]
        ]
        sql_data_source = "update company_data_source set company_name=%s, download_link=%s, spider_name=%s, is_batch=%s, gmt_create=%s, user_create=%s, mark=%s where company_id = %s"
        self.cursor.execute(sql_data_source, parameter_data_source)
        self.conn.commit()
        sql_jud = "select id from company where code = %s"
        self.cursor.execute(sql_jud, item["company_id"])
        results = self.cursor.fetchall()
        if len(results) == 0:
            parameter_company = [
                item["company_id"],
                company_name,
                company_name,
                item["Security_code"],
                "AUS",
                "NSX",
                listing_date,
                "AUD",
                web,
                item["gmt_create"],
                "zx",
                status,
                Security_Type,
                ISIN
            ]
            sql_company = "insert into company(code,name_origin,name_en,security_code,country_code_listed,exchange_market_code,ipo_date,currency_code,website_url,gmt_create,user_create,status,security_type,info_disclosure_id)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            self.cursor.execute(sql_company, parameter_company)
            self.conn.commit()

    def DetailAndDefinition(self, item, param):
        if param is not None:
            for temp in item:
                if temp not in self.exclude_list:
                    name = temp + "_AUS"
                    display_label = temp
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
                    sql_jud = "select id from company_profile_definition where name = %s"
                    self.cursor.execute(sql_jud, name)
                    results = self.cursor.fetchall()
                    if len(results) == 0:
                        sql = "insert into company_profile_definition(name,display_label,data_type,sort,gmt_create,user_create)values(%s,%s,%s,%s,%s,%s)"
                        self.cursor.execute(sql, parameter)
                        self.conn.commit()

            for each in item:
                if each not in self.exclude_list:
                    jud_list = []
                    sql_select = "select id from company_profile_definition where name = %s"
                    self.cursor.execute(sql_select, each + "_AUS")
                    result = self.cursor.fetchone()
                    company_profile_definition_id = result[0]
                    parameter_detail = [
                        company_profile_definition_id,
                        item["company_id"],
                        item[each],
                        item["gmt_create"],
                        "zx"
                    ]
                    sql_jud_detail = "select company_profile_definition_id from company_profile_detail where company_code = %s"
                    self.cursor.execute(sql_jud_detail, item["company_id"])
                    results_jud = self.cursor.fetchall()
                    for each_id in results_jud:
                        if each_id not in jud_list:
                            jud_list.append(each_id[0])
                    if company_profile_definition_id not in jud_list:
                        sql_insert = "insert into company_profile_detail(company_profile_definition_id,company_code,value,gmt_create,user_create)values(%s,%s,%s,%s,%s)"
                        self.cursor.execute(sql_insert, parameter_detail)
                        self.conn.commit()
            self.num += 1
            print(self.num)

    def start_requests(self):
        for each in self.results:
            item = {}
            item_key = []
            NSX_code = each[0]
            item["company_id"] = each[1]
            item["Security_code"] = NSX_code
            link = "https://www.nsx.com.au/marketdata/company-directory/details/" + str(NSX_code) + "/"
            yield scrapy.Request(link, callback=self.parse, meta={"item": item, "item_key": item_key})

    def parse(self, response):
        item = response.meta["item"]
        item_key = response.meta["item_key"]
        pattern = re.compile('class="title">(.*?)</span>([\s\S]*?)<span')
        data = pattern.findall(str(response.body))
        for temp in data:
            key = temp[0].split(":")[0].strip().replace("/", " ").replace(" ", "_").replace("(", "").replace(")","")
            value = temp[1].replace("\n", "").replace("\t", "").replace("People", "").replace("  ", "").replace("\\n", "").replace("\\t", "")
            value = re.sub(r'<.+?>', "", value).replace("u'", "")
            item[key] = value
            item_key.append(key)
        try:
            a = item_key[-1]
        except:
            a = None
        if a == "Announcements":
            item.pop("Announcements")
        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        self.CompanyAndDataSource(item, a)
        self.DetailAndDefinition(item, a)
        # self.conn.close()
        # self.cursor.close()
