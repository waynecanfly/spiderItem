# -*- coding: utf-8 -*-
import time
from scrapy.http import HtmlResponse
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
import requests


# class SeleniumChrome(object):
    # def Initialization(self):
    #     chrome_options = Options()
    #     chrome_options.add_argument("--no-sandbox")
    #     chrome_options.add_argument('--headless')
    #     chrome_options.add_argument('--disable-gpu')
    #     driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
    #     return driver
    #
    # def process_request(self, request, spider):
    #     if "sse" in request.url:
    #         request.headers.setdefault("Referer", "http://www.sse.com.cn/assortment/stock/list/share/")
    #     if spider.name == "announcementSpider":
    #         driver = self.Initialization()
    #         driver.get(request.url)
    #         time.sleep(2)
    #         body = driver.page_source
    #         driver.quit()
    #         return HtmlResponse(url=request.url, body=body, request=request, encoding='utf-8',
    #                             status=200)

jud_list = []


class IP(object):
    def process_request(self, request, spider):
        if request.url not in jud_list:
            jud_list.append(request.url)
        if len(jud_list) == 25:
            response = requests.get(
                "http://api.ip.data5u.com/dynamic/get.html?order=1556f0687ba5dde702aa91e1573b0feb&sep=3"
            )
            ip_port = str(response.text).replace("\n", "")
            request.meta["proxy"] = "http://" + ip_port
            jud_list.clear()
