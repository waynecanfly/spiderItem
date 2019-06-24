# -*- coding: utf-8 -*-
import time
import pymysql
import logging
from datetime import datetime, timedelta
from lxml import etree
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys


num = 0
flag = 0
flag2 = 0
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
formatter = logging.Formatter('%(lineno)d: %(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(console)
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36"')
driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
driver.implicitly_wait(30)


with open("/root/spiderItem/taiwan/record4.txt", "r") as f:
    data = f.read()
    year = int(data.split("@")[0])
    month = int(data.split("@")[1])
    day = int(data.split("@")[-1])
    b = datetime(year, month, day) + timedelta(days=30)


def insertDetail(data, code):
    global flag, flag2
    if flag == 0:
        flag += 1
        parameter = [
            "detail_info_zh_TWN",
            "detail_info_zh",
            "string",
            0,
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
            "zx"
        ]
        sql_jud = "select id from company_profile_definition where name = %s"
        cursor.execute(sql_jud, "detail_info_zh_TWN")
        results = cursor.fetchall()
        if len(results) == 0:
            sql = "insert into company_profile_definition(name,display_label,data_type,sort,gmt_create," \
                  "user_create)values(%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, parameter)
            conn.commit()
    sql_select = "select id from company_profile_definition where name = %s"
    cursor.execute(sql_select, "detail_info_zh_TWN")
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
            data,
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
                with open("/root/spiderItem/taiwan/record4.txt", "w") as g:
                    g.write(newRecord)
            parameter_detail = [
                data,
                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                "zx",
                code,
                company_profile_definition_id
            ]
            sql_update = "update company_profile_detail set value = %s, gmt_update=%s, user_create=%s " \
                         "where company_code = %s and company_profile_definition_id = %s"
            cursor.execute(sql_update, parameter_detail)
            conn.commit()


def insertBasic(data, company_code):
    sql_update = "update company set ipo_date=%s where code=%s and ipo_date = null "
    cursor.execute(sql_update, [data, company_code])
    conn.commit()
    sql = "update company_data_source set mark = 1 where company_id=%s"
    cursor.execute(sql, company_code)
    conn.commit()


sql = "select security_code,company_id from company_data_source where company_id like 'TWN%' and mark=0"
cursor.execute(sql)
result = cursor.fetchall()
driver.get("http://mops.twse.com.tw/mops/web/t05st03")
driver.find_element_by_xpath('//tr[@id="level1"]/td[1]/a[text()="公司基本資料"]').click()
for temp in result:
    try:
        comItem = ""
        detItem = ""
        detailValue = []
        company_code = temp[1]
        driver.find_element_by_xpath('//input[@id="co_id"]').clear()
        driver.find_element_by_xpath('//input[@id="co_id"]').send_keys(temp[0])
        driver.find_element_by_xpath('//input[@id="co_id"]').send_keys(Keys.ENTER)
        time.sleep(8)
        response = etree.HTML(driver.page_source)
        tr_list = response.xpath('//div[@id="table01"]//table[2]//tr')
        for temp in range(1, len(tr_list)):
            data = {}
            th = response.xpath('//div[@id="table01"]//table[2]//tr[' + str(temp) + ']/th')
            if temp != 19:
                for i in range(1, len(th) + 1):
                    key = response.xpath('//div[@id="table01"]//table[2]//tr[' + str(temp) + ']/th[' + str(i) + ']/text()')[0]
                    value = str(response.xpath('//div[@id="table01"]//table[2]//tr[' + str(temp) + ']/td[' + str(i) + ']/text()')
                                [0]).replace(" ", "").replace("\xa0", "").replace("\n", "")
                    data[key] = value
            else:
                key = "特别股发行"
                value = response.xpath('//div[@id="table01"]//table[2]//tr[' + str(temp) + ']/td[1]/text()')[0]
                data[key] = value
                key = "公司债发行"
                value = response.xpath('//div[@id="table01"]//table[2]//tr[' + str(temp) + ']/td[2]/text()')[0]
                data[key] = value
            if temp == 9:
                date = response.xpath('//div[@id="table01"]//table[2]//tr[' + str(temp) + ']/td[2]/text()')[0]
                comItem = str(int(str(date).split("/")[0]) + 1911) + "-" + str(date).split("/")[1] + "-" + str(date).split("/")[-1] + " 00:00:00"
            detailValue.append(data)
        tab2_data = {}
        tab2_data["編製財務報告類型"] = str(response.xpath('//div[@id="table01"]//table[3]//tr[3]/td[1]/text()')[0])\
            .replace(" ", "").replace("\xa0", "").replace("\n", "")
        tab2_data["採用X月制會計年度"] = str(response.xpath('//div[@id="table01"]//table[3]//tr[1]/td[1]/text()')[0])\
            .replace(" ", "").replace("\xa0", "").replace("\n", "")
        tab2_data["在X之前採用X月制會計年度"] = str(response.xpath('//div[@id="table01"]//table[3]//tr[2]/td[1]/text()')[0])\
                                         .replace(" ", "").replace("\xa0", "").replace("\n", "") + "," + \
                                         str(response.xpath('//div[@id="table01"]//table[3]//tr[2]/td[2]/text()')[0]).\
                                         replace(" ", "").replace("\xa0", "").replace("\n", "")
        detailValue.append(tab2_data)
        detItem =str(detailValue)
        insertBasic(comItem, company_code)
        insertDetail(detItem, company_code)
        num += 1
        print(num)
    except IndexError:
        pass
driver.quit()