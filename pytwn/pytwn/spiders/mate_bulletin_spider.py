"""补充由于过载导致下载失败的文件"""

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import BulletinItem


class MateBulletinSpider(scrapy.Spider):
    name = 'mate_bulletin'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MateBulletinSpider, cls).from_crawler(
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
                select report_id, fiscal_year, doc_type, doc_source_url from \
                non_financial_statement_index where country_code='TWN' and \
                is_downloaded is False
            """)
            self.entries = cursor.fetchall()
        conn.close()

        self.total_new = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new', spider.name, self.total_new
        )

    def start_requests(self):
        for each in self.entries:
            url, payload = each['doc_source_url'].split('?')
            yield scrapy.FormRequest(
                url,
                formdata=dict(map(lambda x: x.split('='), payload.split('&'))),
                callback=self.parse,
                errback=self.errback_scraping,
                meta={
                    'report_id': each['report_id'],
                    'fiscal_year': each['fiscal_year'],
                    'doc_type': each['doc_type']
                }
            )

    def parse(self, response):
        self.total_new += 1
        yield BulletinItem(
            report_id=response.meta['report_id'],
            fiscal_year=response.meta['fiscal_year'],
            doc_type=response.meta['doc_type'],
            is_downloaded=response.meta['is_downloaded'],
            data=response.body
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
