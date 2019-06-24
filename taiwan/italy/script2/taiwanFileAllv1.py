# -*- coding: utf-8 -*-
import time
import pymysql
import logging
import random
from datetime import datetime
from lxml import etree
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options


num = 0
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
driver.implicitly_wait(6)
letterList = [chr(i) for i in range(ord("A"), ord("Z") + 1)]


def insertData(item):
    global num
    params = [
        item["country_code"],
        item["exchange_market_code"],
        item["company_code"],
        item["fiscal_year"],
        item["season_type"],
        item["disclosure_date"],
        item["file_name"],
        item["language_written_code"],
        item["doc_type"],
        item["doc_source_url"],
        item["is_doc_url_direct"],
        item["doc_local_path"],
        item["doc_downloaded_timestamp"],
        item["is_downloaded"],
        item["gmt_create"],
        item["user_create"],
        item["report_id"]
    ]
    sql = "insert into financial_statement_index(country_code,exchange_market_code,company_code,fiscal_year," \
          "financial_statement_season_type_code,disclosure_date,file_original_title,language_written_code," \
          "doc_type,doc_source_url,is_doc_url_direct,doc_local_path,doc_downloaded_timestamp,is_downloaded," \
          "gmt_create,user_create,report_id)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, params)
    conn.commit()
    num += 1
    print(num)


def commonItem(item):
    item["exchange_market_code"] = "TWSE"
    if "_" in item["file_name"]:
        item["fiscal_year"] = str(item["file_name"]).split("_")[0]
    else:
        item["fiscal_year"] = item["file_name"]
    item["doc_local_path"] = "/volume3/homes3/TWN/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".html"
    item["country_code"] = "TWN"
    item["doc_type"] = "html"
    item["is_doc_url_direct"] = 1
    item["is_downloaded"] = 1
    item["language_written_code"] = "en"
    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    item["doc_downloaded_timestamp"] = item["gmt_create"]
    item["user_create"] = "zx"
    return item


def uniqueIDMaker():
    time_id = str(datetime.now()).split(".")[-1]
    random_id1 = str(random.randrange(0, 9))
    random_id2 = str(random.randrange(0, 9))
    unique_id = time_id + random_id1 + random_id2
    return unique_id


def saveFile(name):
    data = driver.page_source.replace("\xa0", " ")
    with open("/data/spiderData/taiwan/" + name + ".html", "w", encoding="big5") as f:
        f.write(data)


def beforeIFRS(response, code):
    global driver
    for temp in range(21, 35):
        # name = response.xpath('//div[@id="coltable"]/table//tr[' + str(temp) + ']/td/a/text()')[0]
        driver.find_element_by_xpath('//div[@id="coltable"]/table//tr[' + str(temp) + ']/td/a').click()
        time.sleep(1)
        handles = driver.window_handles
        driver.switch_to.window(handles[-1])
        html = etree.HTML(driver.page_source)
        jud = html.xpath('//table[2]//tr')
        if len(jud) != 0:
            num = html.xpath('//table[2]//tr')[1:]
            for each in range(2, len(num) + 2):
                item = {}
                item["company_code"] = code
                item["season_type"] = str(temp - 15)
                item["report_id"] = code + uniqueIDMaker()
                item["file_name"] = str(html.xpath('//table[2]//tr[' + str(each) + ']/td/a/text()')[0]).replace("/", "_").replace(" ", "")
                date = html.xpath('//table[2]//tr[' + str(each) + ']/td[3]/text()')
                if len(date) != 0:
                    item["disclosure_date"] = str(date[0]).split("/")[0] + "-" + str(date[0]).split("/")[1] + "-" + str(date[0]).split("/")[-1] + " 00:00:00"
                else:
                    item["disclosure_date"] = None
                driver.find_element_by_xpath('//table[2]//tr[' + str(each) + ']/td/a').click()
                item["doc_source_url"] = driver.current_url
                saveFile(item["report_id"])
                item = commonItem(item)
                insertData(item)
                driver.back()
        driver.close()
        driver.switch_to.window(handles[0])
        driver.back()


def afterIFRS(response, code):
    global driver
    for temp in range(3, 8):
        # name = response.xpath('//div[@id="coltable"]/table//tr[' + str(temp) + ']/td/a/text()')[0]
        driver.find_element_by_xpath('//div[@id="coltable"]/table//tr[' + str(temp) + ']/td/a').click()
        time.sleep(1)
        handles = driver.window_handles
        driver.switch_to.window(handles[-1])
        html = etree.HTML(driver.page_source)
        jud = html.xpath('//table[2]//tr')
        if len(jud) != 0:
            num = html.xpath('//table[2]//tr')[1:]
            for each in range(2, len(num) + 2):
                item = {}
                item["company_code"] = code
                item["season_type"] = str(temp - 2)
                item["report_id"] = code + uniqueIDMaker()
                item["file_name"] = str(html.xpath('//table[2]//tr[' + str(each) + ']/td/a/text()')[0]).replace("/", "_").replace(" ", "")
                date = html.xpath('//table[2]//tr[' + str(each) + ']/td[3]/text()')
                if len(date) != 0:
                    item["disclosure_date"] = str(date[0]).split("/")[0] + "-" + str(date[0]).split("/")[1] + "-" +str(date[0]).split("/")[-1] + " 00:00:00"
                else:
                    item["disclosure_date"] = None
                driver.find_element_by_xpath('//table[2]//tr[' + str(each) + ']/td/a').click()
                item["doc_source_url"] = driver.current_url
                saveFile(item["report_id"])
                item = commonItem(item)
                insertData(item)
                driver.back()
        driver.close()
        driver.switch_to.window(handles[0])
        driver.back()


def parse(tr_list, response, letter, page):
    global num, driver
    for temp in range(2, len(tr_list) + 2):
        security_code = response.xpath('//div[@id="coltable"]/table/tbody/tr[' + str(temp) + ']/td[2]/a/text()')[0]
        try:
            sql = "select code from company where security_code=%s and country_code_listed='TWN'"
            cursor.execute(sql, security_code)
            result = cursor.fetchone()
            if result:
                driver.find_element_by_xpath('//div[@id="coltable"]/table/tbody/tr[' + str(temp) + ']/td[2]/a').click()
                html = etree.HTML(driver.page_source)
                afterIFRS(html, result[0])
                beforeIFRS(html, result[0])
                driver.back()
        except Exception as e:
            logger.error(e)
            print(letter, page, security_code, "="*100)
            pass


def main():
    global driver
    driver.get("http://emops.twse.com.tw/server-java/t58query")
    driver.find_element_by_xpath('//div[@id="menu"]//li[3]').click()
    for letter in letterList:
        page = 1
        driver.find_element_by_xpath('//td[@class="word"]/a[text()="' + letter + '"]').click()
        response = etree.HTML(driver.page_source)
        tr_list = response.xpath('//div[@id="coltable"]/table/tbody/tr')[1:]
        pageNum = response.xpath('//span[@id="pnu"]/a[last()]/text()')
        if len(pageNum) != 0:
            pageNum = pageNum[0]
        else:
            pageNum = 1
        if not str(pageNum).isdigit():
            pageNum = response.xpath('//span[@id="pnu"]/a[last()-1]/text()')[0]
        parse(tr_list, response, letter, page)
        while page < int(pageNum):
            page += 1
            driver.find_element_by_xpath('//span[@id="pnu"]/a[text()=">"]').click()
            response2 = etree.HTML(driver.page_source)
            tr_list2 = response2.xpath('//div[@id="coltable"]/table/tbody/tr')[1:]
            parse(tr_list2, response2, letter, page)


if __name__ == "__main__":
    main()
    driver.quit()
