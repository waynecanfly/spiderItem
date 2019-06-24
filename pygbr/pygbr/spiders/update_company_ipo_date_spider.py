"""更新上市公司ipo_date"""

from datetime import datetime

import scrapy
import pymysql
from scrapy import signals
from dateutil.parser import parse as parse_datetime
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived


class UpdateCompanyListSpider(scrapy.Spider):
    name = 'update_company_ipo_date'

    search_url = (
        'http://www.londonstockexchange.com/exchange/searchengine/search.html?'
        'lang=en&q={}'
    )

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
        self.conn = pymysql.connect(**self.settings['DBARGS'])
        with self.conn.cursor() as cursor:
            cursor.execute("""\
                select code, security_code from company where \
                country_code_listed='GBR' and ipo_date is null\
                """)
            self.entries = cursor.fetchall()

        self.total_updated = 0

    def spider_closed(self, spider):
        self.conn.close()
        self.logger.info(
            'Closing spider %s..., %d updated', spider.name,
            self.total_updated
        )

    def start_requests(self):
        for it in self.entries:
            yield scrapy.Request(
                self.search_url.format(it['security_code']),
                callback=self.parse_company_url,
                errback=self.errback_scraping,
                meta={'company': it['code'], 'symbol': it['security_code']}
            )

    def parse_company_url(self, response):
        result = response.xpath(
            "//*[@class='search_results_list']/table/tbody/tr")
        security_code = response.meta['symbol']
        for each in result:
            symbol = each.xpath('./td[1]/strong/text()').extract_first()
            url = each.xpath('./td[2]/a/@href').extract_first()
            if symbol == security_code:
                yield scrapy.Request(
                    url,
                    callback=self.parse_company_info,
                    errback=self.errback_scraping,
                    meta={'company': response.meta['company']}
                )
            break
        else:
            self.logger.warning('Cant\'t find info for %s', security_code)

    def parse_company_info(self, response):
        code = response.meta['company']
        ipo_date = response.xpath(
            "//*[text()='Admission date']/following-sibling::td/text()"
        ).extract_first()
        ipo_date = parse_datetime(ipo_date)
        with self.conn.cursor() as cursor:
            cursor.execute(
                "update company set ipo_date=%s, gmt_update=%s where code=%s",
                (ipo_date, datetime.now(), code)
            )
        self.total_updated += 1

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
