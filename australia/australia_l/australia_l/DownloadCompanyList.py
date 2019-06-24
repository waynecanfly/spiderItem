# -*- coding: utf-8 -*-
import requests
import time
from lxml import etree


class DownloadCompanyList(object):
    def __init__(self, name):
        self.name = name
        if self.name == "ASX":
            self.starturl = "https://www.asx.com.au/asx/research/listedCompanies.do"
            self.resolve = '//div[@id="content"]//a[@target="_blank"]/@href'
            self.HomeLink = "https://www.asx.com.au"
        elif self.name == "NSX":
            self.starturl = "https://www.nsx.com.au/marketdata/directory/"
            self.resolve = '//div[@class="editarea"]/a[@class="blue-link"]/@href'
            self.HomeLink = "https://www.nsx.com.au/"
        else:
            print("所下载国家名字不适用于该程序")
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36"
        }

    def DownloadMain(self):
        html = requests.get(self.starturl, headers=self.headers)
        tree = etree.HTML(html.text)
        link = tree.xpath(self.resolve)[0]
        url = self.HomeLink + link
        response = requests.get(url, headers=self.headers)
        data = response.text
        create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        create_time = str(create_time).replace("-", "").replace(" ", "").replace(":", "")
        with open("/data/OPDCMS/australia/listed_company_update/company_list/" + self.name + "_" + create_time + ".csv", "w") as f:
            if self.name == "NSX":
                f.write(data.replace("\n", ""))
            else:
                f.write(data)
        print("%s下载完成" % (self.name))
