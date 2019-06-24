# -*- coding: utf-8 -*-
import scrapy
import time
from italy.items import ItalydetailItem, ItalyCompanyItem


class ItalyallSpider(scrapy.Spider):
    name = 'italyAll'
    allowed_domains = ['borsaitaliana.it']
    url1 = "https://www.borsaitaliana.it/borsa/azioni/listino-a-z.html?initial="
    url2 = "&lang=en"
    num = 0

    # def addItem(self, companyItem, detailItem, jud):
    #     ItalyCompanyItem = ItalyCompanyItem()
    #     ItalydetailItem = ItalydetailItem()
    #     if jud == 0:
    #         for temp in companyItem:
    #             ItalyCompanyItem[temp] = companyItem[temp]
    #             ItalyCompanyItem[jud] = 0
    #         for each in detailItem:
    #             ItalydetailItem[each] = detailItem[each]
    #             ItalydetailItem[jud] = 0
    #     else:
    #         for temp in companyItem:
    #             ItalyCompanyItem[temp] = companyItem[temp]
    #             ItalyCompanyItem[jud] = 1
    #         for each in detailItem:
    #             ItalydetailItem[each] = detailItem[each]
    #             ItalydetailItem[jud] = 1
    #     return companyItem, detailItem

    # def codefunc(self):
    #     self.num += 1
    #     if self.num < 10:
    #         num = "000" + str(self.num)
    #         code = "ITA1" + num
    #     elif self.num <= 99:
    #         num = "00" + str(self.num)
    #         code = "ITA1" + num
    #     elif self.num <= 999:
    #         num = "0" + str(self.num)
    #         code = "ITA1" + num
    #     elif self.num <= 9999:
    #         code = "ITA1" + str(self.num)
    #     else:
    #         code = "ITA" + str(self.num + 10000)
    #     return code

    def start_requests(self):
        letterList = [chr(i) for i in range(ord("A"), ord("Z")+1)]
        for letter in letterList:
            url = self.url1 + letter + self.url2
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        tr_list = response.xpath('//div[@data-bb-module="list-az-stream"]//table[@class="m-table -firstlevel"]//tr')[2:]
        for temp in tr_list:
            companyItem = ItalyCompanyItem()
            detailItem = ItalydetailItem()
            # companyItem["code"] = self.codefunc()
            companyItem["jud"] = 0
            detailItem["jud"] = 0
            companyItem["country_code_listed"] = "ITA"
            companyItem["exchange_market_code"] = "Borsa Italiana"
            companyItem["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            companyItem["user_create"] = "zx"
            # detailItem["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            # detailItem["user_create"] = "zx"
            companyItem["name"] = str(temp.xpath('./td[1]/a/@title').extract()[0]).replace("\n", "").replace("\t", "").replace("\xa0", "")
            url = "https://www.borsaitaliana.it" + temp.xpath('./td[1]/a/@href').extract()[0]
            yield scrapy.Request(url, callback=self.jump, meta={"companyItem": companyItem, "detailItem": detailItem})
        pageJud = response.xpath('//ul[@class="nav m-pagination__nav"]/li[4]/a/@href').extract()
        if len(pageJud) != 0:
            link = "https://www.borsaitaliana.it" + pageJud[0]
            yield scrapy.Request(link, callback=self.parse)

    def jump(self, response):
        companyItem = response.meta["companyItem"]
        detailItem = response.meta["detailItem"]
        url = "https://www.borsaitaliana.it" + response.xpath('//div[@class="m-dropdown"]//li[2]/a/@href').extract()[0]
        yield scrapy.Request(url, callback=self.allData, meta={"companyItem": companyItem, "detailItem": detailItem})

    def allData(self, response):
        companyItem = response.meta["companyItem"]
        detailItem = response.meta["detailItem"]
        companyItem["security_code"] = str(response.xpath('//table[@class="m-table -clear-m"]//tr[3]/td[2]/span/text()').extract()[0])
        detailItem["security_code"] = companyItem["security_code"]
        yield companyItem
        yield detailItem
        try:
            url = "https://www.borsaitaliana.it" + response.xpath('//div[@class="m-dropdown"]//li[4]/a/@href').extract()[0]
            yield scrapy.Request(url, callback=self.companyProfile, meta={"companyItem": companyItem, "detailItem": detailItem})
        except:
            yield companyItem
            yield detailItem

    def companyProfile(self, response):
        companyItem = response.meta["companyItem"]
        detailItem = response.meta["detailItem"]
        ipoJud = response.xpath('//div[@class="m-dropdown"]/ul/li/a[@title="IPO"]/@href').extract()
        try:
            identifier = str(response.xpath('//iframe[@id="companyprofileframe"]/@src').extract()[0]).split("/")[-1].replace(".htm", "")
            profileUrl = "https://www.borsaitaliana.it/companyprofile/htm/en/" + identifier + ".htm"
            governanceUrl = "https://www.borsaitaliana.it/companyprofile/htm/en/" + identifier + "-corporateGov.htm"
            yield scrapy.Request(profileUrl, callback=self.profileParse,
                                 meta={"companyItem": companyItem, "detailItem": detailItem, "governanceUrl": governanceUrl, "ipoJud": ipoJud})
        except IndexError:
            yield companyItem
            yield detailItem

    def profileParse(self, response):
        ipoJud = response.meta["ipoJud"]
        companyItem = response.meta["companyItem"]
        detailItem = response.meta["detailItem"]
        governanceUrl = response.meta["governanceUrl"]
        detailItem["adress"] = response.xpath('//div[@class="cp__boxdescription__info l-box l-screen -md-10 -sm-10 -pr"]/div[1]/span/text()').extract()[0]
        try:
            detailItem["tel"] = response.xpath(
                '//div[@class="cp__boxdescription__info l-box l-screen -md-10 -sm-10 -pr"]/div[2]/a/text()').extract()[0]
        except:
            detailItem["tel"] = ""
        detailItem["fax"] = str(response.xpath(
            '//div[@class="cp__boxdescription__info l-box l-screen -md-10 -sm-10 -pr"]/div[2]/text()')
                                .extract()[-1]).replace("\r", "").replace("\t", "").replace("\n", "")
        companyItem["web"] = response.xpath('//div[@class="cp__boxdescription__info l-box l-screen -md-10 -sm-10 -pr"]/div[3]/a/@href').extract()[0]
        detailItem["profile"] = response.xpath('//div[@class="l-grid__cell | h-bg--white"]/p/text()').extract()[0]
        companyItem["isin"] = response.xpath('//table[@class="m-table -clear-mtop -clear-mbottom"]//tr[1]/td[2]/span/text()').extract()[0]
        try:
            detailItem["Bloomberg"] = response.xpath('//table[@class="m-table -clear-mtop -clear-mbottom"]//tr[2]/td[2]/span/text()').extract()[0]
        except:
            detailItem["Bloomberg"] = ""
        try:
            detailItem["Reuters"] = response.xpath('//table[@class="m-table -clear-mtop -clear-mbottom"]//tr[3]/td[2]/span/text()').extract()[0]
        except:
            detailItem["Reuters"] = ""
        companyItem["Industry"] = response.xpath('//table[@class="m-table -clear-mtop -clear-mbottom"]//tr[4]/td[2]/span/text()').extract()[0]
        companyItem["sector"] = response.xpath('//table[@class="m-table -clear-mtop -clear-mbottom"]//tr[5]/td[2]/span/text()').extract()[0]
        detailItem["Market"] = response.xpath('//table[@class="m-table -clear-mtop -clear-mbottom"]//tr[6]/td[2]/span/text()').extract()[0]
        yield scrapy.Request(governanceUrl, callback=self.governanceParse,
                             meta={"companyItem": companyItem, "detailItem": detailItem, "ipoJud": ipoJud})

    def governanceParse(self, response):
        data = []
        data2 = []
        data3 = []
        ipoJud = response.meta["ipoJud"]
        companyItem = response.meta["companyItem"]
        detailItem = response.meta["detailItem"]
        companyItem["jud"] = 1
        detailItem["jud"] = 1
        companyItem["Admission_to_Listing"] = None
        tr_list = response.xpath('//table[@class="m-table-consiglieri"]/tbody/tr')
        for temp in tr_list:
            name = str(temp.xpath('./td[4]/span[1]/text()').extract()[0]).replace("\'", "")
            value = str(temp.xpath('./td[4]/span[2]/i/text()').extract()[0]).replace("\'", "")
            nameValue = name + ": " + value
            data.append(nameValue)
        detailItem["BOARD_MEMBERS"] = str(data)
        try:
            value1 = response.xpath('//div[@class="l-grid__cell | h-bg--white"]/span/strong/text()').extract()[0]
            value2 = response.xpath('//div[@class="l-grid__cell | h-bg--white"]/span/i/text()').extract()[0]
            detailItem["BOARD_MEMBERS_OWNERSHIP"] = value1 + " " + value2
        except:
            detailItem["BOARD_MEMBERS_OWNERSHIP"] = ""
        judLenth = response.xpath('//section[@class="l-grid__cell l-screen -md-10"]/article[4]/div[@class="l-grid__cell | h-bg--white"]/span/strong/text()').extract()
        for i in range(1, len(judLenth) + 1):
            value3 = response.xpath('//div[@class="l-grid__cell | h-bg--white"][' + str(i) + ']/span/strong/text()').extract()[-1]
            value4 = response.xpath('//div[@class="l-grid__cell | h-bg--white"][' + str(i) + ']/span/i/text()').extract()[-1]
            e = value3 + ": " + value4
            data3.append(e)
        detailItem["TOP_MANAGEMENT"] = str(data3)
        try:
            detailItem["Compensation"] = response.xpath('//span[@class="checked"]/text()').extract()[0]
        except:
            detailItem["Compensation"] = ""
        try:
            value5 = response.xpath('//div[@class="l-grid__cell | h-bg--white u-pb2"]/span[2]/text()').extract()[-1]
            value6 = response.xpath('//div[@class="l-grid__cell | h-bg--white u-pb2"]/span[2]/span/text()').extract()[0]
            detailItem["List_based_voting_system"] = value5 + " " + value6
        except:
            detailItem["List_based_voting_system"] = ""
        li_list = response.xpath('//ul[@class="legend-azionisti"]/li')
        for each in li_list:
            a = each.xpath('//ul[@class="legend-azionisti"]/li/span[1]/text()').extract()[0]
            b = each.xpath('//ul[@class="legend-azionisti"]/li/span[2]/text()').extract()[0]
            c = a + ": " + b
            data2.append(c)
        detailItem["SHAREHOLDERS"] = str(data2)
        if len(ipoJud) != 0:
            ipo = ipoJud[0]
            url = "https://www.borsaitaliana.it" + ipo
            yield scrapy.Request(url, callback=self.ipoParse, meta={"companyItem": companyItem, "detailItem": detailItem})
        else:
            yield companyItem
            yield detailItem

    def ipoParse(self, response):
        companyItem = response.meta["companyItem"]
        detailItem = response.meta["detailItem"]
        try:
            detailItem["Admission_to_Listing_Market"] = response.xpath(
                '//div[@class="l-box -outside-rl -ptb | h-bg--gray"]/div[1]//table//tr[1]/td[2]/span/text()').extract()[0]
        except:
            detailItem["Admission_to_Listing_Market"] = ""
        detailItem["Nominal_Value"] = response.xpath(
            '//div[@class="l-box -outside-rl -ptb | h-bg--gray"]/div[1]//table//tr[5]/td[2]/span/text()').extract()[0]
        try:
            detailItem["Borsa_Italiana"] = response.xpath(
                '//div[@class="l-box -outside-rl -ptb | h-bg--gray"]/div[1]//table//tr[7]/td[2]/span/text()').extract()[0]
        except:
            detailItem["Borsa_Italiana"] = ""
        try:
            ipo = response.xpath(
                '//div[@class="l-box -outside-rl -ptb | h-bg--gray"]/div[2]//table//tr[1]/td[2]/span/text()').extract()[0]
            companyItem["Admission_to_Listing"] = "20" + str(ipo).split("/")[-1] + "-" + str(ipo).split("/")[0] + "-" + str(ipo).split("/")[1] + " 00:00:00"
            detailItem["Public_Offer_Period"] = response.xpath(
                '//div[@class="l-box -outside-rl -ptb | h-bg--gray"]/div[2]//table//tr[2]/td[2]/span/text()').extract()[0]
        except:
            detailItem["Public_Offer_Period"] = None
        try:
            detailItem["First_Trading_Day"] = response.xpath(
                '//div[@class="l-box -outside-rl -ptb | h-bg--gray"]/div[2]//table//tr[3]/td[2]/span/text()').extract()[0]
        except:
            detailItem["First_Trading_Day"] = ""
        try:
            detailItem["Bookbuilding"] = response.xpath(
                '//div[@class="l-box -outside-rl -ptb | h-bg--gray"]/div[2]//table//tr[4]/td[2]/span/text()').extract()[0]
        except:
            detailItem["Bookbuilding"] = ""
        try:
            detailItem["Offering_Price"] = response.xpath(
                '//div[@class="l-box -outside-rl -ptb | h-bg--gray"]/div[2]//table//tr[5]/td[2]/span/text()').extract()[0]
        except:
            detailItem["Offering_Price"] = ""
        yield companyItem
        yield detailItem
