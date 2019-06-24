# -*- coding: utf-8 -*-
import time
import pymysql
from lxml import etree
from datetime import datetime, timedelta
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options


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
letterList = [chr(i) for i in range(ord("A"), ord("Z") + 1)]

with open("/root/spiderItem/taiwan/record3.txt", "r") as f:
    data = f.read()
    year = int(data.split("@")[0])
    month = int(data.split("@")[1])
    day = int(data.split("@")[-1])
    b = datetime(year, month, day) + timedelta(days=30)

sql_max = "select max(company_id) from company_data_source where company_id like 'TWN%'"
cursor.execute(sql_max)
newstCode = cursor.fetchone()
if newstCode[0]:
    newstNum = int(str(newstCode[0]).replace("TWN", ""))
else:
    newstNum = 10000


def insertDetail(item):
    global flag2, b
    sql = "select company_id from company_data_source where security_code=%s and company_id like %s"
    cursor.execute(sql, [item["security_code"], 'TWN%'])
    code = cursor.fetchone()[0]
    del item["security_code"]
    if flag2 == 0:
        flag2 += 1
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
        parameter_detail = [
            item[each],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
            "zx",
            code,
            company_profile_definition_id
        ]
        sql_jud = "select value from company_profile_detail where company_code = %s and " \
                  "company_profile_definition_id = %s"
        cursor.execute(sql_jud, [code, company_profile_definition_id])
        jud_result = cursor.fetchone()
        if jud_result is not None and b <= datetime.now():
            if str(item[each]) != str(jud_result[0]):
                sql_update = "update company_profile_detail set value = %s, gmt_update=%s, user_create=%s " \
                             "where company_code = %s and company_profile_definition_id = %s"
                cursor.execute(sql_update, parameter_detail)
                conn.commit()
        else:
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


def insertBasic(item):
    global flag, newstNum
    sql = "select company_id from company_data_source where security_code=%s and company_id like %s"
    cursor.execute(sql, [item["security_code"], 'TWN%'])
    result = cursor.fetchone()
    if result:
        if b <= datetime.now():
            if flag == 0:
                flag += 1
                newRecord = str(datetime.now()).split(" ")[0].split("-")[0] + "@" + \
                            str(datetime.now()).split(" ")[0].split("-")[1] + "@" + \
                            str(datetime.now()).split(" ")[0].split("-")[-1]
                with open("/root/spiderItem/taiwan/record3.txt", "w") as g:
                    g.write(newRecord)
            update = [
                item["name"],
                item["sector"],
                item["symbol"],
                item["gmt_create"],
                item["user_create"],
                result[0],
                item["security_code"],
            ]
            sql_update = "update company set name_en=%s,sector_code=%s,info_disclosure_id=%s,gmt_update=%s," \
                         "user_update=%s where code=%s and security_code=%s"
            cursor.execute(sql_update, update)
            conn.commit()
        else:
            pass
    else:
        newstNum += 1
        newCode = "TWN" + str(newstNum)

        # 插入数据到comapany_data_source
        is_batch = 1
        download_link = "http://emops.twse.com.tw/server-java/t58query"
        parameter_data_source = [
            item["name"],
            download_link,
            "taiwanlisten",
            is_batch,
            item["gmt_create"],
            item["user_create"],
            1,
            newCode,
            item["security_code"]
        ]
        sql_data_source = "insert into company_data_source(company_name,download_link,spider_name,is_batch," \
                          "gmt_create,user_create,mark,company_id,security_code)value(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql_data_source, parameter_data_source)
        conn.commit()
        # 插入数据到company表
        params = [
            newCode,
            item["name"],
            item["security_code"],
            item["sector"],
            item["country_code_listed"],
            item["exchange_market_code"],
            item["gmt_create"],
            item["user_create"],
            item["symbol"]
        ]
        company_insert = "insert into company(code,name_en,security_code,sector_code," \
                         "country_code_listed,exchange_market_code,gmt_create," \
                         "user_create,info_disclosure_id)value(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(company_insert, params)
        conn.commit()


def parse(tr_list, response, page):
    global num
    for temp in range(2, len(tr_list) + 2):
        item = {}
        item2 = {}
        item["name"] = response.xpath('//div[@id="coltable"]/table/tbody/tr[' + str(temp) + ']/td[1]/text()')[0]
        item["security_code"] = response.xpath('//div[@id="coltable"]/table/tbody/tr[' + str(temp) + ']/td[2]/a/text()')[0]
        item2["security_code"] = item["security_code"]
        symbol = response.xpath('//div[@id="coltable"]/table/tbody/tr[' + str(temp) + ']/td[3]/text()')
        if len(symbol) != 0:
            item["symbol"] = symbol[0]
        else:
            item["symbol"] = ""
        market = response.xpath('//div[@id="coltable"]/table/tbody/tr[' + str(temp) + ']/td[4]/text()')
        if len(market) != 0:
            item2["market"] = market[0]
        else:
            item2["market"] = ""
        sector = response.xpath('//div[@id="coltable"]/table/tbody/tr[' + str(temp) + ']/td[5]/text()')
        if len(sector) != 0:
            item["sector"] = sector[0]
        else:
            item["sector"] = ""
        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item["user_create"] = "zx"
        item["country_code_listed"] = "TWN"
        item["exchange_market_code"] = "TWSE"
        insertBasic(item)
        insertDetail(item2)
        num += 1
        print("name:%s, page:%s, num:%s" % (item["name"], page, num))


driver.get(url)
time.sleep(3)
for letter in letterList:
    page = 1
    driver.find_element_by_xpath('//td[@class="word"]/a[text()="' + letter + '"]').click()
    time.sleep(3)
    response = etree.HTML(driver.page_source)
    tr_list = response.xpath('//div[@id="coltable"]/table/tbody/tr')[1:]
    pageNum = response.xpath('//span[@id="pnu"]/a[last()]/text()')
    if len(pageNum) != 0:
        pageNum = pageNum[0]
    else:
        pageNum = 1
    if not str(pageNum).isdigit():
        pageNum = response.xpath('//span[@id="pnu"]/a[last()-1]/text()')[0]
    parse(tr_list, response, page)
    while page < int(pageNum):
        page += 1
        driver.find_element_by_xpath('//span[@id="pnu"]/a[text()=">"]').click()
        time.sleep(3)
        response2 = etree.HTML(driver.page_source)
        tr_list2 = response2.xpath('//div[@id="coltable"]/table/tbody/tr')[1:]
        parse(tr_list2, response2, page)
