# -*- coding: utf-8 -*-
import time
from scrapy.http import HtmlResponse
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
# from pyvirtualdisplay import Display
# from selenium.webdriver import Firefox
# from selenium.webdriver.firefox.options import Options
# from xvfbwrapper import Xvfb


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
        if spider.name == "UpdatePro_shanghai_basicinfo":
            driver = self.Initialization()
            driver.get(request.url)
            time.sleep(2)
            body = driver.page_source
            driver.quit()
            return HtmlResponse(url=request.url, body=body, request=request, encoding='utf-8',
                                status=200)

