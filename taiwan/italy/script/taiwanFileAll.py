# -*- coding: utf-8 -*-
import time
import pymysql
import os
from lxml import etree
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options


num = 0
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
driver.implicitly_wait(6)
url = "http://emops.twse.com.tw/server-java/t58query"
letterList = [chr(i) for i in range(ord("A"), ord("Z") + 1)]


def saveFile(name, date, code):
    global num
    print(code, name, date)
    if not os.path.exists("/data/spiderData/taiwan/" + code + "/" + name):
        os.makedirs("/data/spiderData/taiwan/" + code + "/" + name)
    data = driver.page_source.replace("\xa0", " ")
    with open("/data/spiderData/taiwan/" + code + "/" + name + "/" + date + ".html", "w", encoding="big5") as f:
        f.write(data)
    num += 1
    print(num)


def beforeIFRS(response, code):
    global driver
    for temp in range(21, 35):
        name = response.xpath('//div[@id="coltable"]/table//tr[' + str(temp) + ']/td/a/text()')[0]
        driver.find_element_by_xpath('//div[@id="coltable"]/table//tr[' + str(temp) + ']/td/a').click()
        time.sleep(2)
        handles = driver.window_handles
        driver.switch_to.window(handles[-1])
        html = etree.HTML(driver.page_source)
        num = html.xpath('//table[2]//tr')[1:]
        for each in range(2, len(num) + 2):
            date = str(html.xpath('//table[2]//tr[' + str(each) + ']/td/a/text()')[0]).replace("/", "_").replace(" ", "")
            driver.find_element_by_xpath('//table[2]//tr[' + str(each) + ']/td/a').click()
            time.sleep(2)
            saveFile(name, date, code)
            driver.back()
        driver.switch_to.window(handles[0])


def afterIFRS(response, code):
    global driver
    for temp in range(3, 8):
        name = response.xpath('//div[@id="coltable"]/table//tr[' + str(temp) + ']/td/a/text()')[0]
        driver.find_element_by_xpath('//div[@id="coltable"]/table//tr[' + str(temp) + ']/td/a').click()
        time.sleep(3)
        handles = driver.window_handles
        driver.switch_to.window(handles[-1])
        html = etree.HTML(driver.page_source)
        num = html.xpath('//table[2]//tr')[1:]
        for each in range(2, len(num) + 2):
            date = str(html.xpath('//table[2]//tr[' + str(each) + ']/td/a/text()')[0]).replace("/", "_").replace(" ", "")
            driver.find_element_by_xpath('//table[2]//tr[' + str(each) + ']/td/a').click()
            saveFile(name, date, code)
            driver.back()
        driver.switch_to.window(handles[0])


def parse(tr_list, response):
    global num, driver
    for temp in range(2, len(tr_list) + 2):
        security_code = response.xpath('//div[@id="coltable"]/table/tbody/tr[' + str(temp) + ']/td[2]/a/text()')[0]
        sql = "select code from company where security_code=%s and country_code_listed='TWN'"
        cursor.execute(sql, security_code)
        result = cursor.fetchone()
        if result:
            driver.find_element_by_xpath('//div[@id="coltable"]/table/tbody/tr[' + str(temp) + ']/td[2]').click()
            html = etree.HTML(driver.page_source)
            afterIFRS(html, result[0])
            driver.back()
            driver.back()


driver.get(url)
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
    parse(tr_list, response)
    while page < int(pageNum):
        page += 1
        driver.find_element_by_xpath('//span[@id="pnu"]/a[text()=">"]').click()
        response2 = etree.HTML(driver.page_source)
        tr_list2 = response2.xpath('//div[@id="coltable"]/table/tbody/tr')[1:]
        parse(tr_list2, response2)
