"""从nasdaq更新美国上市公司列表"""

import csv
from io import StringIO
from itertools import groupby
from operator import itemgetter
from datetime import datetime
from urllib.parse import quote_plus

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyItem, CompanyDataSourceItem


class UpdateCompanyListSpider(scrapy.Spider):
    name = 'update_company_list'

    data_url = (
        "https://www.nasdaq.com/screening/companies-by-industry.aspx?exchange="
        "{}&render=download"
    )
    symbol_search = (
        "https://www.sec.gov/cgi-bin/browse-edgar?CIK={}&owner=exclude&"
        "action=getcompany&Find=Search"
    )
    name_search = (
        "https://www.sec.gov/cgi-bin/browse-edgar?company={}&owner=exclude&"
        "action=getcompany"
    )
    sec_data_url = (
        "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&"
        "type=&dateb=&owner=exclude&start=0&count=100&output=atom"
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
        conn = pymysql.connect(**self.settings['DBARGS'])
        with conn.cursor() as cursor:
            cursor.execute("""\
                select code, security_code, info_disclosure_id from company \
                where country_code_listed='USA'\
                """)
            records = cursor.fetchall()
        conn.close()

        self.ciks = [it['info_disclosure_id'] for it in records]
        self.symbols = [it['security_code'] for it in records]
        NUMBER = slice(3, None)  # 公司code数字编号区
        self.max_code_num = int(max(it['code'] for it in records)[NUMBER])

        self.total_new = 0
        self.found_ciks = 0
        self.unfound_ciks = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %s new companies, %s found cik, %s not.',
            spider.name, self.total_new, self.found_ciks, self.unfound_ciks
        )

    def start_requests(self):
        for exchange in ['NASDAQ', 'NYSE', 'AMEX']:
            yield scrapy.Request(
                self.data_url.format(exchange),
                callback=self.parse,
                errback=self.errback_scraping,
                meta={'exchange': exchange, 'download_timeout': 300}
            )

    def parse(self, response):
        reader = csv.DictReader(StringIO(response.text))
        # 同一家公司只搜索一个股票代码
        for company, items in groupby(reader, itemgetter('Name')):
            it = sorted(items, key=itemgetter('Symbol'))[0]
            symbol = it['Symbol']
            if it['IPOyear'].isdigit():
                ipo_date = datetime(int(it['IPOyear']), 1, 1)
            else:
                ipo_date = None
            if symbol not in self.symbols:  # 通过股票代码判断新公司
                self.total_new += 1
                yield scrapy.Request(
                    self.symbol_search.format(symbol),
                    callback=self.search_cik,
                    errback=self.errback_scraping,
                    meta={
                        'symbol': symbol,
                        'name': company,
                        'exchange': response.meta['exchange'],
                        'ipo_date': ipo_date,
                        'first': True
                    }
                )

    def search_cik(self, response):
        symbol = response.meta['symbol']
        name = response.meta['name']
        exchange = response.meta['exchange']
        company = response.css('.companyName')
        if company:
            self.found_ciks += 1
            company_name = company.xpath('./text()').extract_first().strip()
            cik = company.xpath('./a/text()').re(r'\d+')[0]
            self.max_code_num += 1
            company_code = 'USA' + str(self.max_code_num)
            yield CompanyItem(
                code=company_code, name_origin=company_name,
                name_en=company_name, security_code=response.meta['symbol'],
                info_disclosure_id=cik, ipo_date=response.meta['ipo_date'],
                exchange_market_code=response.meta['exchange']
            )
            yield CompanyDataSourceItem(
                company_id=company_code, company_name=company_name,
                security_code=response.meta['symbol'], info_disclosure_id=cik,
                download_link=self.sec_data_url.format(cik),
            )
        else:
            if 'first' in response.meta:  # 使用股票代码未搜索到CIK，使用公司名搜索
                yield scrapy.Request(
                    self.name_search.format(quote_plus(response.meta['name'])),
                    callback=self.search_cik,
                    errback=self.errback_scraping,
                    meta={
                        'symbol': response.meta['symbol'],
                        'name': response.meta['name'],
                        'exchange': response.meta['exchange'],
                        'ipo_date': response.meta['ipo_date']
                    }
                )
            if 'first' not in response.meta:  # 使用公司命及股票代码均搜索不到CIK
                self.unfound_ciks += 1
                self.logger.warning(
                    '(%s, %s, %s) has no direct cik.', symbol, name, exchange
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
