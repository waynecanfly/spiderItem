"""补充未下载成功的文件"""

import os
from datetime import datetime

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived


class BondDocsMateSpider(scrapy.Spider):

    name = 'docs_mate'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BondDocsMateSpider, cls).from_crawler(
            crawler, *args, **kwargs
        )
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        self.logger.info('Opening spider %s...', spider.name)
        self.conn = pymysql.connect(**self.settings['DBARGS'])
        with self.conn.cursor() as cursor:
            cursor.execute("""\
                select * from securities_statement_index where \
                user_create='lq' and is_downloaded=0 and note is null\
            """)
            self.records = cursor.fetchall()

    def spider_closed(self, spider):
        self.logger.info('Closing spider %s...', spider.name)
        self.conn.close()

    def start_requests(self):
        for it in self.records:
            yield scrapy.Request(
                it['doc_source_url'],
                callback=self.parse,
                errback=self.errback_scraping,
                meta={'item': it}
            )

    def parse(self, response):
        item = response.meta['item']
        file_year = str(item['disclosure_date']).split('-')[0]
        file_dir = os.path.join(self.settings['FILES_STORE'], file_year)
        filename = item['report_id'] + '.' + item['doc_type']
        if not os.path.exists(file_dir):
            os.mkdir(file_dir)
        filepath = os.path.join(file_dir, filename)
        with open(filepath, 'wb') as f:
            f.write(response.body)

        vals = (
            True, datetime.now(),
            self.settings['DOC_PATH'].format(file_year, filename),
            item['report_id']
        )
        with self.conn.cursor() as cursor:
            cursor.execute("""\
                update securities_statement_index set is_downloaded=%s, \
                doc_downloaded_timestamp=%s, doc_local_path=%s where \
                report_id=%s """, vals)

    def errback_scraping(self, failure):
        request = failure.request
        req_url = failure.request.url
        if failure.check(HttpError):
            response = failure.value.response
            if response.status == 404:
                with self.conn.cursor() as cursor:
                    cursor.execute("""\
                        update securities_statement_index set note=%s where \
                        report_id=%s\
                    """, (404, request.meta['item']['report_id']))
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
