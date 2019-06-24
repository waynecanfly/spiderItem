"""通过Recently Listed下的公司信息更新某些当前上市公司的ipo_date"""

import json
from json import JSONDecodeError
from datetime import datetime

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyItem


class UpdateLatestIpoDateSpider(scrapy.Spider):
    name = 'update_latest_ipo_date'

    start_urls = [
        'https://www.tsx.com/json/company-directory/recent/tsx',
        'https://www.tsx.com/json/company-directory/recent/tsxv'
    ]

    markets = ['tsx', 'tsxv']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateLatestIpoDateSpider, cls).from_crawler(
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
                select code, security_code, exchange_market_code, ipo_date \
                from company where country_code_listed='CAN'\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = {}
        for it in records:
            id_ = it['exchange_market_code'], it['security_code']
            self.companies[id_] = it['code'], it['ipo_date']

        self.total_updated = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d updated', spider.name, self.total_updated
        )

    def parse(self, response):
        try:
            companies = json.loads(response.text)['results']
        except JSONDecodeError:
            companies = []

        exchange = response.url.split('/')[-1].upper()
        for each in companies:
            id_ = (exchange, each['symbol'])
            if id_ in self.companies:
                ipo_date = datetime.utcfromtimestamp(
                    each['date']).strftime('%Y-%m-%d')
                company_code, ipo_date_db = self.companies[id_]
                if ipo_date != ipo_date_db:
                    self.total_updated += 1
                    yield CompanyItem(
                        code=company_code, ipo_date=ipo_date,
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
