# -*- coding: utf-8 -*-
import requests
import time
import re
import pymysql
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import Select
from spiderAPI import forQuery, uniqueID


num = 0
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
conn2 = pymysql.connect(host="10.100.4.100", port=3306, db="Standard_database", user="root", passwd="OPDATA", charset="utf8")
cursor2 = conn2.cursor()
# sql = "select name_origin,code from company where country_code_listed='GBR'"
# cursor.execute(sql)
# result = cursor.fetchall()
qData = forQuery.get_params()
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')


def saveFile(fileType, fileName, data):
    if fileType == "html":
        name = "/data/OPDCMS/GBR/" + fileName + ".html"
    else:
        name = "/data/OPDCMS/GBR/" + fileName + ".pdf"
    with open(name, "wb") as f:
        f.write(data)


def requestFile(url, fileType, fileName):
    response = requests.get(url, timeout=180)
    data = response.content
    saveFile(fileType, fileName, data)


def insertData(item):
    global num
    sql_jud = "select id from GBR where doc_source_url=%s"
    cursor2.execute(sql_jud, item["doc_source_url"])
    result = cursor2.fetchone()
    if not result:
        params = [
            item["country_code"],
            item["exchange_market_code"],
            item["company_code"],
            item["fiscal_year"],
            item["financial_statement_season_type_code"],
            item["disclosure_date"],
            item["language_written_code"],
            item["doc_type"],
            item["doc_source_url"],
            item["is_doc_url_direct"],
            item["doc_local_path"],
            item["doc_downloaded_timestamp"],
            item["is_downloaded"],
            item["gmt_create"],
            item["user_create"],
            item["report_id"],
            item["announcement_type"],
            item["detail_type"],
            item["file_name"]
        ]
        sql = "insert into GBR(country_code,exchange_market_code," \
              "company_code,fiscal_year,financial_statement_season_type_code," \
              "disclosure_date,language_written_code,doc_type,doc_source_url,is_doc_url_direct," \
              "doc_local_path,doc_downloaded_timestamp,is_downloaded,gmt_create,user_create,report_id," \
              "announcement_type,detail_type,file_original_title)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor2.execute(sql, params)
        conn2.commit()
        # requestFile(item["doc_source_url"], item["doc_type"], item["report_id"])
        num += 1
        print(num)
    else:
        print("已存在")


def paramsFunc(item, date, source, temp):
    link = temp[4]
    item["doc_source_url"] = "http://tools.morningstar.co.uk" + link
    item["report_id"] = item["company_code"] + uniqueID.uniqueIDMaker()
    item["exchange_market_code"] = source
    item["fiscal_year"] = str(date).split("/")[-1]
    jud = str(item["doc_source_url"]).split("=")[-1]
    if len(jud) == 9:
        item["doc_type"] = "pdf"
        item["doc_local_path"] = "/volum3/homes3/GBR/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".pdf"
    else:
        item["doc_type"] = "html"
        item["doc_local_path"] = "/volum3/homes3/GBR/" + str(item["fiscal_year"]) + "/" + item["report_id"] + ".html"
    item["country_code"] = "GBR"
    item["is_doc_url_direct"] = 1
    item["is_downloaded"] = 1
    item["language_written_code"] = "en"
    item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    item["doc_downloaded_timestamp"] = item["gmt_create"]
    item["user_create"] = "zx"
    item["file_name"] = temp[5]
    insertData(item)


def dataProcessing(data_list, detail_type, financial_statement_season_type_code, announcement_type):
    for temp in data_list:
        company_name = str(temp[3]).replace("&amp;", "&")
        sql = "select code from company where name_origin=%s and country_code_listed='GBR'"
        cursor.execute(sql, company_name)
        result = cursor.fetchone()
        if result:
            item = {}
            item["company_code"] = result[0]
            item["detail_type"] = detail_type
            item["financial_statement_season_type_code"] = financial_statement_season_type_code
            item["announcement_type"] = announcement_type
            date = temp[0]
            source = temp[2]
            item["disclosure_date"] = str(date).split("/")[-1] + "-" + str(date).split("/")[1] + "-" + \
                                      str(date).split("/")[0] + " " + temp[1]
            if source == "RNS":
                sql = "select min(disclosure_date) from financial_statement_index where country_code = 'GBR' and company_code = %s"
                cursor.execute(sql, item["company_code"])
                result = cursor.fetchone()
                if result:
                    try:
                        minDate = int(str(result[0]).replace("-", "").replace(" ", "").replace(":", ""))
                        nowDate = int(str(item["disclosure_date"]).replace("-", "").replace(" ", "").replace(":", ""))
                        if nowDate < minDate:
                            paramsFunc(item, date, source, temp)
                    except:
                        paramsFunc(item, date, source, temp)
                else:
                    paramsFunc(item, date, source, temp)
            else:
                paramsFunc(item, date, source, temp)


def getDataList(driver):
    time.sleep(3)
    body = driver.page_source
    data_list = re.findall(
        'class="gridDocumentsDate"><a>(.*?)</a>.*?class="gridDocumentsTime"><a>(.*?)</a>.*?class='
        '"gridDocumentsSource"><a>(\w+?)</a>.*?class="gridDocumentsCompanyName">.*?>(.*?)</a>.*?'
        'href="(/tsweu6nqxu/.*?DocumentId=\d+?)".*?>(.*?)<', str(body))
    return data_list


def pageTurning(page, data_list, detail_type, financial_statement_season_type_code, announcement_type, driver):
    if len(data_list) == 500:
        page += 1
        js = "__doPostBack('ctl00$ContentPlaceHolder1$pgrDocumentList', '" + str(page) + "'" + ")"
        driver.execute_script(js)
        dataList = getDataList(driver)
        dataProcessing(dataList, detail_type, financial_statement_season_type_code, announcement_type)
        pageTurning(page, dataList, detail_type, financial_statement_season_type_code, announcement_type, driver)


def main():
    for each in qData:
        try:
            page = 1
            detail_type = each["Dt"]
            financial_statement_season_type_code = each["St"]
            announcement_type = each["At"]
            # chrome_options.add_argument('--proxy-server=http://171.37.135.94:8123')
            driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
            driver.get("http://tools.morningstar.co.uk/tsweu6nqxu/globaldocuments/list/default.aspx")
            driver.execute_script("document.getElementById('ctl00_ContentPlaceHolder1_txtDateFrom').value='01/01/2008'")
            driver.execute_script("document.getElementById('ctl00_ContentPlaceHolder1_txtDateTo').value='25/09/2018'")
            a = Select(driver.find_element_by_xpath('//select[@id="ctl00_ContentPlaceHolder1_ddlHeadlineType"]'))
            a.select_by_value(each["Qt"])
            # a.select_by_value("QRF")
            driver.find_element_by_id("ctl00_ContentPlaceHolder1_btnGo").click()
            data_list = getDataList(driver)
            if len(data_list) != 0:
                b = Select(driver.find_element_by_id("ctl00_ContentPlaceHolder1_ddlPageSize"))
                b.select_by_value("500")
                data_list = getDataList(driver)
                dataProcessing(data_list, detail_type, financial_statement_season_type_code, announcement_type)
                pageTurning(page, data_list, detail_type, financial_statement_season_type_code, announcement_type, driver)
            else:
                print(each["Qt"])
            driver.quit()
        except Exception as e:
            print("="*20)
            print(e)
            print("="*20)


if __name__ == '__main__':
    main()
