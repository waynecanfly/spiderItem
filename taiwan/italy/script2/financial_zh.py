# -*- coding: utf-8 -*-
import pymysql
import random
import time
import os
import json
import requests
from lxml import etree
from datetime import datetime
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options


num = 0
jud = 0
cnt = 0
second_list = [123, 134, 143, 158, 171, 166, 180]
data = [
{"type": "20", "url_list": ["http://doc.twse.com.tw/server-java/t57sb01?step=1&colorchg=1&co_id=", "&year=", "&seamon=&mtype=A&"]},
{"type": "21", "url_list": ["http://doc.twse.com.tw/server-java/t57sb01_1?step=1&colorchg=1&co_id=", "&year=", "&mtype=G&"]},
{"type": "25", "url_list": ["http://doc.twse.com.tw/server-java/t57sb01?step=1&colorchg=1&co_id=", "&year=", "&mtype=F&"]},
{"type": "22", "url_list": ["http://doc.twse.com.tw/server-java/t57sb01?step=1&colorchg=1&co_id=", "&year=&seamon=&mtype=B&"]},
{"type": "23", "url_list": ["http://doc.twse.com.tw/server-java/t57sb01?step=1&colorchg=1&co_id=", "&year=&seamon=&mtype=B&dtype=B1d&"]},
{"type": "24", "url_list": ["http://doc.twse.com.tw/server-java/t57sb01_2?step=1&colorchg=1&year=", "&seamon=&mtype=B&"]}
]
type1 = ["20", "21", "25"]
type2 = ["22", "23"]
conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
cursor = conn.cursor()
sql = "select security_code, code from company where country_code_listed = 'TWN' and id > 63190 order by id"
cursor.execute(sql)
results = cursor.fetchall()


def getProxy():
    while 1:
        msg = requests.get("http://api.ip.data5u.com/dynamic/get.html?order=d0cf16d2a6861a9d8f0c064eff15828a&json=1")
        proxy = "{0}{ip}:{port}".format("http://", **json.loads(msg.text)["data"][0])
        r = requests.get("http://mops.twse.com.tw/mops/web/t57sb01_q1", proxies={"http": proxy})
        print("{}, {}".format(proxy, r.status_code))
        if r.status_code == 200:
            return proxy
        else:
            time.sleep(0.6)


def browserMaker():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument("{}{}".format("--proxy-server=", getProxy()))
    chrome_options.add_argument(
        'user-agent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36"')
    driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': '/data/spiderData/tw'}}
    driver.execute("send_command", params)
    driver.implicitly_wait(30)
    return driver


def uniqueIDMaker():
    time_id = str(datetime.now()).split(".")[-1]
    random_id1 = str(random.randrange(0, 9))
    random_id2 = str(random.randrange(0, 9))
    unique_id = time_id + random_id1 + random_id2
    return unique_id


def insertData(item):
    global num
    params = [
        item["code"],
        item["report_id"],
        item["data_year"],
        item["data_type"],
        item["case_type"],
        item["nature"],
        item["data_details"],
        item["remarks"],
        item["file_size"],
        item["upload_date"],
        item["gmt_create"],
        item["user_create"],
        item["doc_source_url"],
        item["doc_type"],
        item["doc_local_path"]
    ]
    sql = "insert into twn_financial_file_zh(code,report_id,data_year,data_type,case_type,nature,data_details," \
          "remarks,file_size,upload_date,gmt_create,user_create,doc_source_url,doc_type,doc_local_path)" \
          "value (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(sql, params)
    conn.commit()
    num += 1
    print(num)


def saveFile(url, name, doc_type):
    a = requests.get(url)
    a.encoding = "GBK"
    with open("/data/spiderData/tw/" + name + "." + doc_type, "wb") as f:
        f.write(a.content)


def forFileType(driver, i):
    html = etree.HTML(driver.page_source)
    name = html.xpath('//table[@border="5"]//tr[' + str(i) + ']/td[8]/a/text()')[0]
    doc_type = str(name).split(".")[-1]
    return doc_type, name


def parse(response, i, driver, code):
    global cnt
    cnt += 1
    if cnt % 10 == 0:
        a = random.choice(second_list)
        print("{} {} {}".format("Randomly wait", a, "seconds"))
        time.sleep(a)
    item = {}
    item["code"] = code
    item["report_id"] = item["code"] + uniqueIDMaker()
    item["data_year"] = str(response.xpath('//table[@border="5"]//tr[' + str(i) + ']/td[2]/text()')[0]).replace(
        "\xa0", "").replace(" ", "")
    item["data_type"] = str(response.xpath('//table[@border="5"]//tr[' + str(i) + ']/td[3]/text()')[0]).replace(
        "\xa0", "").replace(" ", "")
    item["case_type"] = str(response.xpath('//table[@border="5"]//tr[' + str(i) + ']/td[4]/text()')[0]).replace(
        "\xa0", "").replace(" ", "")
    item["nature"] = str(response.xpath('//table[@border="5"]//tr[' + str(i) + ']/td[5]/text()')[0]).replace("\xa0",
        "").replace(" ", "")
    item["data_details"] = str(response.xpath('//table[@border="5"]//tr[' + str(i) + ']/td[6]/text()')[0]).replace(
        "\xa0", "").replace(" ", "")
    item["remarks"] = str(response.xpath('//table[@border="5"]//tr[' + str(i) + ']/td[7]/text()')[0]).replace(
        "\xa0", "").replace(" ", "")
    item["file_size"] = str(response.xpath('//table[@border="5"]//tr[' + str(i) + ']/td[9]/text()')[0]).replace(
        "\xa0", "").replace(" ", "")
    item["upload_date"] = str(response.xpath('//table[@border="5"]//tr[' + str(i) + ']/td[10]/text()')[0]).replace(
        "\xa0", " ")
    item["gmt_create"] = str(datetime.now()).split(".")[0]
    item["user_create"] = "zx"
    driver.find_element_by_xpath('//table[@border="5"]//tr[' + str(i) + ']/td[8]/a').click()
    data = forFileType(driver, i)
    if data[0] == "doc" or data[0] == "zip":
        item["doc_source_url"] = ""
        item["doc_type"] = data[0]
        item["doc_local_path"] = "/volume3/homes3/TWN_zh/" + item["report_id"] + "." + data[0]
        time.sleep(1)
        os.rename("/data/spiderData/tw/t57sb01", "/data/spiderData/tw/" + item["report_id"] + "." + str(data[1]).split(".")[-1])
    else:
        handles = driver.window_handles
        driver.switch_to.window(handles[-1])
        time.sleep(1)
        html = etree.HTML(driver.page_source)
        item["doc_source_url"] = "http://doc.twse.com.tw" + html.xpath('//a/@href')[0]
        item["doc_type"] = str(item["doc_source_url"]).split(".")[-1].replace('"', "").replace("'", "")
        item["doc_local_path"] = "/volume3/homes3/TWN_zh/" + item["report_id"] + "." + item["doc_type"]
        saveFile(item["doc_source_url"], item["report_id"], item["doc_type"])
        driver.close()
        driver.switch_to.window(handles[0])
    return item


def main():
    cur_year = datetime.now().year
    # driver = browserMaker()
    for each in data:
        if each["type"] in type1:
            for temp in results:
                for j in range(96, int(cur_year) - 1910):
                    url = each["url_list"][0] + temp[0] + each["url_list"][1] + str(j) + each["url_list"][2]
                    driver = browserMaker()
                    try:
                        driver.get(url)
                        time.sleep(1)
                        response = etree.HTML(driver.page_source)
                        tr_list = response.xpath('//table[@border="5"][1]//tr')[1:]
                        for i in range(2, len(tr_list) + 2):
                            try:
                                item = parse(response, i, driver, temp[1])
                                insertData(item)
                            except:
                                with open("/root/spiderItem/taiwan/italy/script2/loss.txt", "a")as f:
                                    f.write("{}@{}@{}{}".format(url, i, temp[1], "\n"))
                                print("{}@{}@{}".format(url, i, temp[1]))
                                pass
                    except:
                        with open("/root/spiderItem/taiwan/italy/script2/loss.txt", "a")as f:
                            f.write("{}@{}@{}{}".format(url, "#*#", temp[1], "\n"))
                        print("{}@{}@{}".format(url, "#*#", temp[1]))
                        pass
                    driver.quit()

        elif each["type"] in type2:
            for temp in results:
                url = each["url_list"][0] + temp[0] + each["url_list"][1]
                driver = browserMaker()
                try:
                    driver.get(url)
                    time.sleep(1)
                    response = etree.HTML(driver.page_source)
                    tr_list = response.xpath('//table[@border="5"][1]//tr')[1:]
                    for i in range(2, len(tr_list) + 2):
                        try:
                            item = parse(response, i, driver, temp[1])
                            insertData(item)
                        except:
                            with open("/root/spiderItem/taiwan/italy/script2/loss.txt", "a")as f:
                                f.write("{}@{}@{}{}".format(url, i, temp[1], "\n"))
                            print("{}@{}@{}".format(url, i, temp[1]))
                            pass
                except:
                    with open("/root/spiderItem/taiwan/italy/script2/loss.txt", "a")as f:
                        f.write("{}@{}@{}{}".format(url, "#*#", temp[1], "\n"))
                    print("{}@{}@{}".format(url, "#*#", temp[1]))
                    pass
                driver.quit()
        """以下表的结构和上面的不太一样，数据只有一条先不做处理"""
        # else:
        #     for j in range(96, int(cur_year) - 1910):
        #         url = each["url_list"][0] + str(j) + each["url_list"][1]
        #         driver = browserMaker()
        #         driver.get(url)
        #         time.sleep(1)
        #         response = etree.HTML(driver.page_source)
        #         tr_list = response.xpath('//table[@border="5"][1]//tr')[1:]
        #         print(len(tr_list), "+" * 100)
        #         for i in range(2, len(tr_list) + 2):
        #             item = parse(response, i, driver, 1)
        #             insertData(item)
        #


if __name__ == "__main__":
    main()