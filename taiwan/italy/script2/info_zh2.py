# -*- coding: utf-8 -*-
import time
import pymysql
import random
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
chrome_options = Options()

chrome_options.add_argument("--no-sandbox")
# chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36"')
driver = Chrome(executable_path="D:\install\chromedriver\chromedriver.exe", chrome_options=chrome_options)
# driver.maximize_window()
driver.implicitly_wait(30)


def insertData(item):
    global num
    params = [
        item["exchange_market_code"],
        item["company_code"],
        item["disclosure_date"],
        item["language_written_code"],
        item["doc_type"],
        item["doc_local_path"],
        item["is_downloaded"],
        item["gmt_create"],
        item["user_create"],
        item["report_id"],
        item["belong_2_type"]
    ]
    sql = "insert into twn_info(exchange_market_code,company_code,disclosure_date," \
          "language_written_code,doc_type,doc_local_path,is_downloaded,gmt_create,user_create,report_id, belong_2_type)" \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, params)
    conn.commit()
    num += 1
    print(num)


def commonItem(item):
    item["exchange_market_code"] = "TWSE"
    item["doc_local_path"] = "/volume3/homes3/twnInfo/" + item["report_id"] + ".html"
    item["doc_type"] = "html"
    item["is_downloaded"] = 1
    item["language_written_code"] = "zh-tw"
    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    item["user_create"] = "zx"
    item["belong_2_type"] = "歷年變更登記"
    return item


def uniqueIDMaker():
    time_id = str(datetime.now()).split(".")[-1]
    random_id1 = str(random.randrange(0, 9))
    random_id2 = str(random.randrange(0, 9))
    unique_id = time_id + random_id1 + random_id2
    return unique_id


def saveFile(code, date):
    item = {}
    item["report_id"] = code + uniqueIDMaker()
    item["disclosure_date"] = date
    item["company_code"] = code
    handles = driver.window_handles
    driver.switch_to.window(handles[-1])
    data = driver.page_source
    time.sleep(1)
    with open("D:/temp/" + item["report_id"] + ".html", "w", encoding="utf-8") as f:
        f.write(data)
    driver.close()
    item = commonItem(item)
    insertData(item)
    driver.switch_to.window(handles[0])


sql = "select security_code,company_id from company_data_source where company_id like 'TWN%'"
cursor.execute(sql)
result = cursor.fetchall()
driver.get("http://mops.twse.com.tw/mops/web/t05st05")
for temp in result:
    company_code = temp[1]
    driver.find_element_by_xpath('//input[@id="co_id"]').clear()
    driver.find_element_by_xpath('//input[@id="co_id"]').send_keys(temp[0])
    driver.find_element_by_xpath('//input[@id="co_id"]').send_keys(Keys.ENTER)
    html = etree.HTML(driver.page_source)
    time.sleep(0.5)
    tr_list = len(html.xpath('//form[@name="fm_t05st05"]//table//tr')) - 1
    for temp in range(2, tr_list + 2):
        date = str(html.xpath('//div[@id="table01"]//table//tr[' + str(temp) + ']/td[1]/text()')[0]).replace("\n", "")
        driver.find_element_by_xpath('//div[@id="table01"]//table//tr[' + str(temp) + ']/td[2]').click()
        saveFile(company_code, date)
driver.quit()