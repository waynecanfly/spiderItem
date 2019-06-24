# -*- coding: utf-8 -*-
import time
import os
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select


create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
create_time = str(create_time).replace("-", "").replace(" ", "").replace(":", "")
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36"')
driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': '/data/OPDCMS/india/listed_company_update/company_list'}}
command_result = driver.execute("send_command", params)
driver.implicitly_wait(30)
print("response from browser:")
for key in command_result:
    print("result:" + key + ":" + str(command_result[key]))

driver.get("https://www.bseindia.com/corporates/List_Scrips.aspx?expandable=1")
s1 = Select(driver.find_element_by_id('ContentPlaceHolder1_ddSegment'))
s1.select_by_value("Equity")
driver.find_element_by_id("ContentPlaceHolder1_btnSubmit").click()
time.sleep(2)
driver.find_element_by_id("ContentPlaceHolder1_lnkDownload").click()
time.sleep(15)
os.rename("/data/OPDCMS/india/listed_company_update/company_list/ListOfScrips.csv",
          "/data/OPDCMS/india/listed_company_update/company_list/BSE_" + create_time + ".csv")
print("已完成下载")
driver.close()
