"""下载、更新公司韩文列表"""

import csv
from io import StringIO

import scrapy
import pymysql
from scrapy import FormRequest
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyItem


class UpdateCompanyListSpider(scrapy.Spider):
    """补充公司的韩语名称"""
    name = 'update_company_list_ko'

    start_urls = [(
        'http://marketdata.krx.co.kr/contents/MKD/04/0406/04060100/'
        'MKD04060100.jsp')]

    otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    file_url = 'http://file.krx.co.kr/download.jspx'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyListSpider, cls).from_crawler(
            crawler, *args, **kwargs
        )
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        self.logger.info('Opening spider %s...', spider.name)
        conn = pymysql.connect(**self.settings['DBARGS'])
        with conn.cursor() as cursor:
            cursor.execute("""\
                select code, security_code from company where \
                country_code_listed='KOR' and name_origin is null\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = {
            it['security_code']: it['code'] for it in records
        }

        self.total_updated = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d updated', spider.name, self.total_updated
        )

    def parse(self, response):
        """使用data-bld信息获取用于下载公司列表文件所需的code值"""
        bld = response.xpath('//form/@data-bld').extract_first()
        request = FormRequest.from_response(
            response,
            formdata={
                'name': 'fileDown', 'filetype': 'csv', 'url': bld,
            },
            callback=self.search_companies,
            errback=self.errback_scraping
        )
        request = request.replace(url=self.otp_url)
        yield request

    def search_companies(self, response):
        data_code = response.text
        yield FormRequest(
            self.file_url,
            formdata={'code': data_code},
            callback=self.parse_companies,
            errback=self.errback_scraping
        )

    def parse_companies(self, response):
        reader = csv.reader(StringIO(response.text))
        next(reader)
        for row in reader:
            for index, value in enumerate(row):
                if index == 1:
                    symbol = value
                elif index == 2:
                    name = value
            if symbol in self.companies:
                self.total_updated += 1
                company_code = self.companies[symbol]
                yield CompanyItem(
                    code=company_code, name_origin=name
                )

    def errback_scraping(self, failure):
        req_url = failure.request.url
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error('HttpError %s on %s', response.status, req_url)
        elif failure.check(DNSLookupError):
            self.logger.error('DNSLookupError on %s', req_url)
        elif failure.check(ConnectionRefusedError):
            self.logger.error('ConnectionRefusedError on %s', req_url)
        elif failure.check(TimeoutError, TCPTimedOutError):
            self.logger.error('TimeoutError on %s', req_url)
        elif failure.check(ResponseNeverReceived):
            self.logger.error('ResponseNeverReceived on %s', req_url)
        else:
            self.logger.error('UnpectedError on %s', req_url)
            self.logger.error(repr(failure))
