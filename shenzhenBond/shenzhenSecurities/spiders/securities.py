# -*- coding: utf-8 -*-
import scrapy
import time
from shenzhenSecurities.items import ShenzhensecuritiesItemS


class SecuritiesSpider(scrapy.Spider):
    name = 'securities'
    allowed_domains = ['cninfo.com.cn']
    start_urls = ['http://www.cninfo.com.cn/cninfo-new/information/bondlist']

    def parse(self, response):
        data_list = response.xpath('//div[@id="con-a-5"]//li')
        for temp in data_list:
            security_code = str(temp.xpath('./a/text()').extract()[0]).split(" ")[0]
            link = "http://www.cninfo.com.cn/information/bond/brief/" + str(security_code) + ".html"
            yield scrapy.Request(link, callback=self.detailParse)

    def detailParse(self, response):
        item = ShenzhensecuritiesItemS()
        item["security_code"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[1]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["short_name"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[2]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["name_origin"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[3]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["name_en"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[4]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["security_type"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[5]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["security_form"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[6]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["interest_payment_method"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[7]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["start_interest_date"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[8]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["end_date"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[9]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["redemption_date"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[10]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["repayment_period"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[11]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["interest_date_description"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[12]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["ipo_date"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[13]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["delisting_date"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[14]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["unit_denomination"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[15]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["interest_rate_type"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[16]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["coupon_rate"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[17]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["interest_rate_start_date"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[18]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["interest_rate_end_date"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][1]//tr[19]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["issue_object"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][2]//tr[1]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["issue_price"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][2]//tr[2]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["issue_start_date"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][2]//tr[3]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["issue_end_date"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][2]//tr[4]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["actual_circulation"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][2]//tr[5]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        item["issuance_method"] = \
            str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][2]//tr[6]/td[2]/text()').extract()[
                    0]).replace("\r\n", "")
        try:
            item["issuance_fee_rate"] = \
                str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][2]//tr[7]/td[2]/text()').extract()[
                        0]).replace("\r\n", "")
            item["distribution_method"] = \
                str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][2]//tr[8]/td[2]/text()').extract()[
                        0]).replace("\r\n", "")
            item["tender_date"] = \
                str(response.xpath('//div[@class="zx_left"]/div[@class="clear"][2]//tr[9]/td[2]/text()').extract()[
                        0]).replace("\r\n", "")
        except:
            item["issuance_fee_rate"] = ""
            item["distribution_method"] = ""
            item["tender_date"] = ""
        item["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item["exchange_market_code"] = "SZSE"
        item["country_code_origin"] = "CHN"
        item["user_create"] = "zx"
        item["short_name_sz"] = "债券简称"
        item["security_form_sz"] = "债券形式"
        item["interest_payment_method_sz"] = "付息方式"
        item["start_interest_date_sz"] = "起息日"
        item["end_date_sz"] = "到期日"
        item["redemption_date_sz"] = "兑付日"
        item["repayment_period_sz"] = "偿还期限（月）"
        item["interest_date_description_sz"] = "付息日期说明"
        item["delisting_date_sz"] = "终止上市日期"
        item["unit_denomination_sz"] = "单位面值（元）"
        item["interest_rate_type_sz"] = "利率类型"
        item["coupon_rate_sz"] = "票面利率（%）"
        item["interest_rate_start_date_sz"] = "利率开始适用日期"
        item["interest_rate_end_date_sz"] = "利率终止适用日期"
        item["issue_object_sz"] = "发行对象"
        item["issue_price_sz"] = "发行价格（元）"
        item["issue_start_date_sz"] = "发行起始日期"
        item["issue_end_date_sz"] = "发行终止日期"
        item["actual_circulation_sz"] = "实际发行量（亿元）"
        item["issuance_method_sz"] = "发行方式"
        item["issuance_fee_rate_sz"] = "发行手续费率（%）"
        item["distribution_method_sz"] = "分销方式"
        item["tender_date_sz"] = "招标日"
        yield item