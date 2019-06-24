# -*- coding: utf-8 -*-
import time
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import Select


chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
driver.get("http://tools.morningstar.co.uk/tsweu6nqxu/globaldocuments/list/default.aspx")
time.sleep(2)
driver.find_element_by_id("ctl00_ContentPlaceHolder1_txtCompany").send_keys("ACTIVE ENERGY GROUP PLC")
driver.execute_script("document.getElementById('ctl00_ContentPlaceHolder1_txtDateFrom').value='01/01/2008'")
driver.execute_script("document.getElementById('ctl00_ContentPlaceHolder1_txtDateTo').value='25/09/2018'")
a = Select(driver.find_element_by_xpath('//select[@id="ctl00_ContentPlaceHolder1_ddlHeadlineType"]'))
a.select_by_value("MSC")
driver.find_element_by_id("ctl00_ContentPlaceHolder1_btnGo").click()
# driver.execute_script("__doPostBack('ctl00$ContentPlaceHolder1$pgrDocumentList','2')")
body = driver.page_source
print(body)
driver.quit()