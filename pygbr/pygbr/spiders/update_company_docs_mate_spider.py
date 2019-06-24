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


class UpdateCompanyDocsMateSpider(scrapy.Spider):
    name = 'update_company_docs_mate'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyDocsMateSpider, cls).from_crawler(
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
                select report_id, financial_statement_season_type_code period,\
                doc_source_url, doc_type from financial_statement_index where \
                is_downloaded=0 and user_create='lq' and country_code='GBR'\
                union all select report_id, acc_no period, doc_source_url, \
                doc_type from non_financial_statement_index where \
                is_downloaded=0 and user_create='lq' and country_code='GBR'\
                """)
            self.entries = cursor.fetchall()

        self.total_downloaded = 0

    def spider_closed(self, spider):
        self.conn.close()
        self.logger.info(
            'Closing spider %s..., %d downloaded.', spider.name,
            self.total_downloaded
        )

    def start_requests(self):
        for it in self.entries:
            yield scrapy.Request(
                it['doc_source_url'],
                callback=self.parse,
                errback=self.errback_scraping,
                meta={
                    'report_id': it['report_id'],
                    'doc_type': it['doc_type'],
                    'period': it['period']
                }
            )

    def parse(self, response):
        self.total_downloaded += 1
        filename = response.meta['report_id'] + '.' + response.meta['doc_type']
        filepath = os.path.join(self.settings['FILES_STORE'], filename)
        with open(filepath, 'wb') as f:
            f.write(response.body)

        if response.meta['period']:
            table = 'financial_statement_index'
        else:
            table = 'non_financial_statement_index'
        vals = (
            True, self.settings['DOC_PATH'].format(filename),
            datetime.now(), datetime.now(), 'lq', response.meta['report_id']
        )
        with self.conn.cursor() as cursor:
            cursor.execute("""\
                update {} set is_downloaded=%s, doc_local_path=%s, \
                doc_downloaded_timestamp=%s, gmt_update=%s, user_update=%s \
                where report_id=%s""".format(table), vals)

    def errback_scraping(self, failure):
        req_url = failure.request.url
        if failure.check(HttpError):
            response = failure.value.response
            if response.status != 404:
                self.logger.error(
                    'HttpError %s on %s', response.status, req_url)
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
