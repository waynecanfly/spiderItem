"""辅助下载需要验证码的文件"""

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import AnnounceItem, ReportItem


class DocDownloaderSpider(scrapy.Spider):

    name = 'docs_downloader'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(DocDownloaderSpider, cls).from_crawler(
            crawler, *args, **kwargs
        )
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        self.logger.info('Opening spider %s...', spider.name)
        conn = pymysql.connect(**self.settings['DBARGS'])

        sql = """\
            select report_id, fiscal_year, doc_type, doc_source_url from {} \
            where country_code='can' and is_downloaded is FALSE and \
            doc_type='pdf'\
        """
        tables = [
            'financial_statement_index', 'non_financial_statement_index']
        self.entries = {}
        with conn.cursor() as cursor:
            for table in tables:
                cursor.execute(sql.format(table))
                self.entries[table] = cursor.fetchall()

    def spider_closed(self, spider):
        self.logger.info('Closing spider %s,', spider.name)

    def start_requests(self):
        count = 0
        stop = False
        for table, items in self.entries.items():
            if stop:
                break
            for each in items:
                if count == 2000:
                    stop = True
                    break
                count += 1
                yield scrapy.Request(
                    each['doc_source_url'],
                    callback=self.parse,
                    errback=self.errback_scraping,
                    meta={
                        'url': each['doc_source_url'],
                        'table': table,
                        'report_id': each['report_id'],
                        'doc_type': each['doc_type'],
                        'fiscal_year': str(each['fiscal_year']),
                    }
                )

    def parse(self, response):
        if 'html' in response.headers[b'Content-Type'].decode():
            pass
        else:
            if response.meta['table'].startswith('non'):
                yield AnnounceItem(
                    report_id=response.meta['report_id'],
                    doc_type=response.meta['doc_type'],
                    fiscal_year=response.meta['fiscal_year'],
                    table=response.meta['table'],
                    data=response.body,
                )
            else:
                yield ReportItem(
                    report_id=response.meta['report_id'],
                    doc_type=response.meta['doc_type'],
                    fiscal_year=response.meta['fiscal_year'],
                    table=response.meta['table'],
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
