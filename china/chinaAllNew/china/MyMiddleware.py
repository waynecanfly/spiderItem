# -*- coding: utf-8 -*-
import time
import requests
from scrapy.http import HtmlResponse
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from china.items import ChinaIntroItem_shanghai,ChinaIntroItem_shenzhen
# from pyvirtualdisplay import Display
# from selenium.webdriver import Firefox
# from selenium.webdriver.firefox.options import Options
# from xvfbwrapper import Xvfb

jud_list = []


class SeleniumChrome(object):
    def Initialization(self):
        # xvfb = Xvfb(width=1280, height=720)
        # xvfb.start()
        # options = Options()
        # options.add_argument('-headless')  # 无头参数
        # # driver = Firefox(executable_path='D:\install\geckodriver/geckodriver.exe', firefox_options=options)
        # driver = Firefox(executable_path='/root/firefox/geckodriver', firefox_options=options)

        # display = Display(visible=0, size=(800, 800))
        # display.start()
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
        return driver

    # def __del__(self):
    #     driver = self.Initialization()
    #     # xvfb = self.Initialization()[1]
    #     driver.quit()
    #     # xvfb.stop()

    def process_request(self, request, spider):
        ua = UserAgent()
        agent = ua.random
        request.headers['User-Agent'] = agent
        if "sse" in request.url:
            request.headers.setdefault("Referer", "http://www.sse.com.cn/assortment/stock/list/share/")
            # if request.url not in jud_list:
            #     jud_list.append(request.url)
            # if len(jud_list) == 10:
            #     response = requests.get(
            #         "http://api.ip.data5u.com/dynamic/get.html?order=1556f0687ba5dde702aa91e1573b0feb&sep=3"
            #     )
            #     ip_port = str(response.text).replace("\n", "")
            #     request.meta["proxy"] = "http://" + ip_port
            #     jud_list.clear()
        if spider.name == "china_updateAll_c":
            driver = self.Initialization()
            driver.get(request.url)
            time.sleep(2)
            body = driver.page_source
            driver.quit()
            return HtmlResponse(url=request.url, body=body, request=request, encoding='utf-8',
                                status=200)
