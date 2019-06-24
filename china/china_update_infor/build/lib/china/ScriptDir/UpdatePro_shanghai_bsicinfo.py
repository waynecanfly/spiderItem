# -*- coding: utf-8 -*-
import scrapy
import time
import pymysql
from scrapy_splash import SplashRequest
from china.items import ChinaIntroItem


class CodeSpider(scrapy.Spider):
    name = 'UpdatePro_shanghai_basicinfo_old'
    allowed_domains = ['sse.com.cn']
    #start_urls = ["http://www.sse.com.cn/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=600112"]
    num_list = []
    data_list = []
    conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
    cursor = conn.cursor()
    sql = "select security_code,code from company where code like " + "'CHN%'"
    cursor.execute(sql)
    results = cursor.fetchall()

    def start_requests(self):
        for temp in self.results:
            code = temp[0]
            company_id = temp[1]
            if int(code) >= 600000:
                data = str(code) + "@" + str(company_id)
                self.data_list.append(data)
        for each in self.data_list:
            time.sleep(4.5)
            code = str(each).split("@")[0]
            company_id = str(each).split("@")[-1]
            url = "http://www.sse.com.cn/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=" + str(code)
            lua_script = """
            function main(splash)
              local url = splash.args.url
              assert(splash:go(url))
              assert(splash:wait(2))
              return {
                html = splash:html(),
                cookies = splash:get_cookies(),
              }
            end
            """
            yield SplashRequest(url, self.parse,
                                endpoint='execute',
                                args={'wait': 1, 'lua_source': lua_script}, meta={"url": url, "company_id": company_id})

    def parse(self, response):
        link = response.meta["url"]
        company_id = response.meta["company_id"]
        lua_script = '''
        function main(splash)
          splash:init_cookies(splash.args.cookies)
          local url = splash.args.url
          assert(splash:go(url))
          assert(splash:wait(2))
          return {
            html = splash:html(),
          }
        end
        '''
        yield SplashRequest(link, self.parse_result,
                            endpoint='execute',
                            args={'wait': 1, 'lua_source': lua_script}, dont_filter=True, meta={"company_id": company_id})

    def parse_result(self, response):
        company_id = response.meta["company_id"]
        item = ChinaIntroItem()
        #网页爬取的数据--company
        item["security_code"] = response.xpath('//table[@class="table search_"]/tbody/tr[1]/td/text()').extract()[0]
        ipo_date = str(response.xpath('//table[@class="table search_"]//tr[3]/td/a[@target="_blank"]/text()').extract()[0]).split("/")[0]
        item["ipo_date"] = str(ipo_date) + " 00:00:00"
        item["name_origin"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[6]/td/text()').extract()[0]).split("/")[0]
        item["name_en"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[6]/td/text()').extract()[0]).split("/")[-1]
        website_url = response.xpath('//table[@class="table search_"]/tbody/tr[13]/td/a/text()').extract()
        if len(website_url) == 0:
            item["website_url"] = None
        else:
            item["website_url"] = website_url[0]
        item["status"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[17]/td/text()').extract()[0]).split("/")[0]
        #自定义添加的数据--company
        item["code"] = company_id
        item["country_code_listed"] = "CHN"
        item["country_code_origin"] = "CHN"
        item["exchange_market_code"] = "SSE"
        item["currency_code"] = "CNY"
        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item["user_create"] = "root"

        #网页爬取的数据--detail-value
        item["convertible_bonds_for_short"] = response.xpath('//table[@class="table search_"]/tbody/tr[4]/td/text()').extract()[0]
        item["company_short_name_zh"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[5]/td/text()').extract()[0]).split("/")[0]
        item["company_short_name_en"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[5]/td/text()').extract()[0]).split("/")[-1]
        item["registered_address"] = response.xpath('//table[@class="table search_"]/tbody/tr[7]/td/text()').extract()[0]
        item["mailing_address"] = response.xpath('//table[@class="table search_"]/tbody/tr[8]/td/text()').extract()[0]
        item["legal_representative"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[9]/td/text()').extract()[0]).replace(" ", "")
        item["secretary_name"] = response.xpath('//table[@class="table search_"]/tbody/tr[10]/td/text()').extract()[0]
        item["e_mail"] = response.xpath('//table[@class="table search_"]/tbody/tr[11]/td/a/text()').extract()[0]
        item["phone_number"] = response.xpath('//table[@class="table search_"]/tbody/tr[12]/td/text()').extract()[0]
        item["CSRC_industry"] = response.xpath('//table[@class="table search_"]/tbody/tr[14]/td/text()').extract()[0]
        item["SSE_industry"] = response.xpath('//table[@class="table search_"]/tbody/tr[15]/td/text()').extract()[0]
        item["district_belong_to"] = response.xpath('//table[@class="table search_"]/tbody/tr[16]/td/text()').extract()[0]
        item["is_SSE_180_sample_stock"] = response.xpath('//table[@class="table search_"]/tbody/tr[18]/td/text()').extract()[0]
        item["is_overseas_listing"] = response.xpath('//table[@class="table search_"]/tbody/tr[19]/td/text()').extract()[0]
        item["overseas_listing_land"] = response.xpath('//table[@class="table search_"]/tbody/tr[20]/td/text()').extract()[0]
        # 网页爬取的数据--detail-title
        item["convertible_bonds_for_short_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[4]/th/text()').extract()[0]
        item["company_short_name_zh_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[5]/th/text()').extract()[0]
        item["company_short_name_en_title"] = item["company_short_name_zh_title"]
        item["registered_address_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[7]/th/text()').extract()[0]
        item["mailing_address_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[8]/th/text()').extract()[0]
        item["legal_representative_title"] = str(response.xpath('//table[@class="table search_"]/tbody/tr[9]/th/text()').extract()[0]).replace(" ", "")
        item["secretary_name_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[10]/th/text()').extract()[0]
        item["e_mail_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[11]/th/text()').extract()[0]
        item["phone_number_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[12]/th/text()').extract()[0]
        item["CSRC_industry_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[14]/th/text()').extract()[0]
        item["SSE_industry_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[15]/th/text()').extract()[0]
        item["district_belong_to_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[16]/th/text()').extract()[0]
        item["is_SSE_180_sample_stock_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[18]/th/text()').extract()[0]
        item["is_overseas_listing_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[19]/th/text()').extract()[0]
        item["overseas_listing_land_title"] = response.xpath('//table[@class="table search_"]/tbody/tr[20]/th/text()').extract()[0]
        yield item
        """"
        except:
            self.num_list.append("1")
            num1 = len(self.num_list)
            print(num1)
            
        for temp in item:
            print(temp)
            print(str(item[temp]).encode("utf-8"))
            """
