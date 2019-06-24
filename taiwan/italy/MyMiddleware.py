# -*- coding: utf-8 -*-
import time
from scrapy.http import HtmlResponse
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent


class SeleniumChrome(object):
    def Initialization(self):
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
        return driver

    def process_request(self, request, spider):
        if spider.name == "taiwanlistzh":
            driver = self.Initialization()
            driver.get(request.url)
            time.sleep(1)
            driver.find_element_by_xpath('//div[@class="reportCont"]/b/button').click()
            time.sleep(1)
            handles = driver.window_handles
            driver.switch_to.window(handles[-1])
            time.sleep(9)
            body = driver.page_source
            driver.quit()
            return HtmlResponse(url=request.url, body=body, request=request, encoding='utf-8',
                                status=200)
