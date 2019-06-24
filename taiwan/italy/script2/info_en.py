# -*- coding: utf-8 -*-
import time
import pymysql
from lxml import etree
from datetime import datetime, timedelta
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys


num = 0
flag = 0
flag2 = 0
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
url = "http://emops.twse.com.tw/server-java/t58query"
sql = "SELECT security_code, company_id FROM company_data_source where mark = 0 and company_id like 'TWN%'"
cursor.execute(sql)
results = cursor.fetchall()

with open("/root/spiderItem/taiwan/record2.txt", "r") as f:
    data = f.read()
    year = int(data.split("@")[0])
    month = int(data.split("@")[1])
    day = int(data.split("@")[-1])
    b = datetime(year, month, day) + timedelta(days=30)


def insertDetail(item, code):
    global flag, flag2
    if flag == 0:
        flag += 1
        for temp in item:
            parameter = [
                temp + "_TWN",
                temp,
                "string",
                0,
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                "zx"
            ]
            sql_jud = "select id from company_profile_definition where name = %s"
            cursor.execute(sql_jud, temp + "_TWN")
            results = cursor.fetchall()
            if len(results) == 0:
                sql = "insert into company_profile_definition(name,display_label,data_type,sort,gmt_create," \
                      "user_create)values(%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, parameter)
                conn.commit()
    for each in item:
        sql_select = "select id from company_profile_definition where name = %s"
        cursor.execute(sql_select, each + "_TWN")
        result = cursor.fetchone()
        company_profile_definition_id = result[0]
        sql_jud = "select value from company_profile_detail where company_code = %s and " \
                  "company_profile_definition_id = %s"
        cursor.execute(sql_jud, [code, company_profile_definition_id])
        jud_result = cursor.fetchone()
        if jud_result is None:
            parameter_d = [
                company_profile_definition_id,
                code,
                item[each],
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                "zx"
            ]
            sql_insert = "insert into company_profile_detail(company_profile_definition_id,company_code," \
                         "value,gmt_create,user_create)values(%s,%s,%s,%s,%s)"
            cursor.execute(sql_insert, parameter_d)
            conn.commit()
        else:
            if b <= datetime.now():
                if flag2 == 0:
                    flag2 += 1
                    newRecord = str(datetime.now()).split(" ")[0].split("-")[0] + "@" + \
                                str(datetime.now()).split(" ")[0].split("-")[1] + "@" + \
                                str(datetime.now()).split(" ")[0].split("-")[-1]
                    with open("/root/spiderItem/taiwan/record2.txt", "w") as g:
                        g.write(newRecord)
                parameter_detail = [
                    item[each],
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                    "zx",
                    code,
                    company_profile_definition_id
                ]
                sql_update = "update company_profile_detail set value = %s, gmt_update=%s, user_create=%s " \
                             "where company_code = %s and company_profile_definition_id = %s"
                cursor.execute(sql_update, parameter_detail)
                conn.commit()


def insertBasic(item, company_code):
    update = [
        item["ipo_date"],
        item["Web_Address"],
        item["gmt_create"],
        item["user_create"],
        company_code
    ]
    sql_update = "update company set ipo_date=%s,website_url=%s,gmt_update=%s," \
                 "user_update=%s where code=%s"
    cursor.execute(sql_update, update)
    conn.commit()
    sql = "update company_data_source set mark = 1 where company_id=%s"
    cursor.execute(sql, company_code)
    conn.commit()


def parse(response, company_code):
    global num
    item = {}
    item2 = {}
    ipo_date = response.xpath('//div[@id="company_e"]//tbody/tr[7]/td[2]/text()')[0]
    item["ipo_date"] = str(ipo_date).split("/")[0] + "-" + str(ipo_date).split("/")[1] + "-" + str(ipo_date).split("/")[-1] + " 00:00:00"
    item2["Place_of_Incorporation_of_Foreign_Companies"] = response.xpath('//div[@id="company_e"]//tbody/tr[8]/td[2]/text()')[0]
    item2["Chairman"] = response.xpath('//div[@id="company_e"]//tbody/tr[10]/td[2]/text()')[0]
    item2["General_Manager"] = response.xpath('//div[@id="company_e"]//tbody/tr[11]/td[2]/text()')[0]
    item2["Spokesman"] = response.xpath('//div[@id="company_e"]//tbody/tr[12]/td[2]/text()')[0]
    item2["Title_of_Spokesman"] = response.xpath('//div[@id="company_e"]//tbody/tr[13]/td[2]/text()')[0]
    item2["Fiscal_Year_end"] = response.xpath('//div[@id="company_e"]//tbody/tr[15]/td[2]/text()')[0]
    item2["Accounting_Firm"] = response.xpath('//div[@id="company_e"]//tbody/tr[16]/td[2]/text()')[0]
    item2["Chartered_Public_Accountant1"] = response.xpath('//div[@id="company_e"]//tbody/tr[17]/td[2]/text()')[0]
    item2["Chartered_Public_Accountant2"] = response.xpath('//div[@id="company_e"]//tbody/tr[18]/td[2]/text()')[0]
    item2["Address"] = response.xpath('//div[@id="company_e"]//tbody/tr[20]/td[2]/text()')[0]
    item2["Telephone"] = response.xpath('//div[@id="company_e"]//tbody/tr[22]/td[2]/text()')[0]
    item2["Fax"] = response.xpath('//div[@id="company_e"]//tbody/tr[23]/td[2]/text()')[0]
    item2["Email_Address"] = response.xpath('//div[@id="company_e"]//tbody/tr[24]/td[2]/text()')[0]
    item["Web_Address"] = response.xpath('//div[@id="company_e"]//tbody/tr[25]/td[2]/a/@href')[0]
    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    item["user_create"] = "zx"
    insertBasic(item, company_code)
    insertDetail(item2, company_code)
    num += 1
    print(num)


for temp in results:
    driver.get(url)
    time.sleep(3)
    driver.find_element_by_xpath('//input[@id="pro_co_id"]').send_keys(str(temp[0]))
    driver.find_element_by_xpath('//input[@id="pro_co_id"]').send_keys(Keys.ENTER)
    handles = driver.window_handles
    driver.switch_to.window(handles[-1])
    time.sleep(3)
    parse(etree.HTML(driver.page_source), temp[1])