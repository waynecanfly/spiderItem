# -*- coding: utf-8 -*-
import requests
import time
import re
import pymysql
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import Select
from spiderAPI import forQuery


num = 0
nowYear = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))).split("-")[0]
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
qData = forQuery.get_params()
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')


def dataProcessing(data_list, detail_type, financial_statement_season_type_code, announcement_type, newstdate, newsturl):
    flag = 0
    for temp in data_list:
        source = temp[2]
        if source == "RNS":
            pass
        else:
            doc_source_url = "http://tools.morningstar.co.uk" + temp[4]
            date = temp[0]
            disclosure_date = str(date).split("/")[-1] + "-" + str(date).split("/")[1] + "-" + \
                                      str(date).split("/")[0] + " " + temp[1]
            if doc_source_url not in newsturl and disclosure_date >= newstdate:
                fiscal_year = str(date).split("/")[-1]
                file_name = temp[5]
                company_name = str(temp[3]).replace("&amp;", "&")
                global num
                params = [
                    doc_source_url,
                    disclosure_date,
                    fiscal_year,
                    file_name,
                    source,
                    company_name,
                    detail_type,
                    financial_statement_season_type_code,
                    announcement_type,
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                    "zx"
                ]
                sql = "insert into gbr_not_parsing(doc_source_url,disclosure_date,fiscal_year,file_original_title,jud_type,company_name," \
                      "detail_type,financial_statement_season_type_code,announcement_type,gmt_create,user_create)" \
                      "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql, params)
                conn.commit()
                num += 1
                print(num)
            else:
                break
    return flag


def getDataList(driver):
    time.sleep(3)
    body = driver.page_source
    data_list = re.findall(
        'class="gridDocumentsDate"><a>(.*?)</a>.*?class="gridDocumentsTime"><a>(.*?)</a>.*?class='
        '"gridDocumentsSource"><a>(\w+?)</a>.*?class="gridDocumentsCompanyName">.*?>(.*?)</a>.*?'
        'href="(/tsweu6nqxu/.*?DocumentId=\d+?)".*?>(.*?)<', str(body))
    return data_list


def pageTurning(page, data_list, detail_type, financial_statement_season_type_code, announcement_type, driver, newstdate, newsturl):
    if len(data_list) == 500:
        page += 1
        js = "__doPostBack('ctl00$ContentPlaceHolder1$pgrDocumentList', '" + str(page) + "'" + ")"
        driver.execute_script(js)
        dataList = getDataList(driver)
        flag = dataProcessing(dataList, detail_type, financial_statement_season_type_code, announcement_type, newstdate, newsturl)
        if flag == 0:
            pageTurning(page, dataList, detail_type, financial_statement_season_type_code, announcement_type, driver, newstdate, newsturl)


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
            if each["Dt"] == "":
                sql = "select disclosure_date,doc_source_url from financial_statement_index where disclosure_date " \
                             "in(select max(disclosure_date) as disclosure_date from financial_statement_index where " \
                             "country_code = 'GBR' and user_create ='zx' and financial_statement_season_type_code = %s " \
                      "and exchange_market_code != %s) and country_code = 'GBR' and user_create ='zx' and " \
                      "financial_statement_season_type_code = %s and exchange_market_code != %s"
                cursor.execute(sql, [each["St"], "RNS", each["St"], "RNS"])
                fr = cursor.fetchall()
                if len(fr) != 0:
                    newstdate = str(fr[0][0])
                    newsturl = [i[1] for i in fr]
                else:
                    newstdate = '2008-01-01'
                    newsturl = []
            else:
                sql = "select disclosure_date,doc_source_url from non_financial_statement_index where disclosure_date " \
                      "in(select max(disclosure_date) as disclosure_date from non_financial_statement_index where " \
                      "country_code = 'GBR' and user_create ='zx' and Non_financial_announcement_detail_type = %s and exchange_market_code != %s) and " \
                      "country_code = 'GBR' and user_create ='zx' and Non_financial_announcement_detail_type = %s and exchange_market_code != %s"
                cursor.execute(sql, [each["Dt"], "RNS", each["Dt"], "RNS"])
                fr = cursor.fetchall()
                if len(fr) != 0:
                    newstdate = str(fr[0][0])
                    newsturl = [i[1] for i in fr]
                else:
                    newstdate = '2008-01-01'
                    newsturl = []
            startTime = "01/01/" + newstdate.split("-")[0]
            endTime = "31/12/" + str(nowYear)
            driver.execute_script("document.getElementById('ctl00_ContentPlaceHolder1_txtDateFrom').value=" + "'" + startTime + "'")
            driver.execute_script("document.getElementById('ctl00_ContentPlaceHolder1_txtDateTo').value=" + "'" + endTime + "'")
            a = Select(driver.find_element_by_xpath('//select[@id="ctl00_ContentPlaceHolder1_ddlHeadlineType"]'))
            a.select_by_value(each["Qt"])
            driver.find_element_by_id("ctl00_ContentPlaceHolder1_btnGo").click()
            data_list = getDataList(driver)
            if len(data_list) != 0:
                b = Select(driver.find_element_by_id("ctl00_ContentPlaceHolder1_ddlPageSize"))
                b.select_by_value("500")
                data_list = getDataList(driver)
                flag = dataProcessing(data_list, detail_type, financial_statement_season_type_code, announcement_type, newstdate, newsturl)
                if flag == 0:
                    pageTurning(page, data_list, detail_type, financial_statement_season_type_code, announcement_type, driver, newstdate, newsturl)
            else:
                print(each["Qt"], startTime, endTime)
            driver.quit()
        except Exception as e:
            print("="*20)
            print(e)
            print("="*20)


if __name__ == '__main__':
    main()
