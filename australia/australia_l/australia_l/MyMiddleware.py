# -*- coding: utf-8 -*-
import time
import requests
from scrapy.http import HtmlResponse
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options


class SeleniumChrome(object):
    def Initialization(self):
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        # driver = Chrome(executable_path="D:\install\chromedriver/chromedriver.exe", chrome_options=chrome_options)
        driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
        return driver

    def process_request(self, request, spider):
        if spider.name == "BasicInfo_ASX":
            driver = self.Initialization()
            driver.get(request.url)
            # time.sleep(2)
            body = driver.page_source
            driver.quit()
            return HtmlResponse(url=request.url, body=body, request=request, encoding='utf-8',
                                status=200)
