import re
import json

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

    # it starts at https://www.euronext.com/listings/issuers-directory
    data_fetch_url = (
        'https://www.euronext.com/pd/stocks/data?formKey=nyx_pd_filter_values'
    )

    formdata = {
        'sEcho': '2',
        'iColumns': '7',
        'iDisplayStart': '0',
        'iDisplayLength': '20',
    }

    markets = {
        'Euronext Access Paris': 'EAP',
        'Euronext Growth Paris': 'EGP',
        'Euronext Paris': 'EP'
    }

    financial_calendar_url = (
        'https://www.euronext.com/en/financial-calendar-company/ajax?'
        'instrument_id='
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
                select code, security_code, isin from company where \
                country_code_listed='FRA'\
            """)
            records = cursor.fetchall()
        conn.close()

        self.members = [(it['security_code'], it['isin']) for it in records]
        if records:
            NUMBER = slice(3, None)  # 公司code数字编号区
            self.max_code_num = int(max(it['code'] for it in records)[NUMBER])
        else:
            self.max_code_num = 10000

        self.total_new = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new companies', spider.name,
            self.total_new
        )

    def start_requests(self):
        yield scrapy.FormRequest(
            self.data_fetch_url,
            method='POST',
            formdata=self.formdata,
            callback=self.parse,
            errback=self.errback_scraping,
        )

    def parse(self, response):
        jres = json.loads(response.body)
        companies = jres['aaData']
        if not companies:
            return

        for it in companies:
            quote, isin, symbol, market, _, _, _ = it
            if 'Paris' in market and (symbol, isin) not in self.members:
                self.total_new += 1
                url, name = re.search(
                    r'href="(.*?)".*?>(.+?)<', quote).groups()
                self.max_code_num += 1
                try:
                    market_code = self.markets[market]
                except KeyError:
                    for m in market.split(','):
                        if m.strip() in self.markets:
                            market_code = self.markets[m.strip()]
                            break
                    else:
                        market_code = None
                company_code = 'FRA' + str(self.max_code_num)
                yield CompanyItem(
                    code=company_code, name_origin=name, security_code=symbol,
                    exchange_market_code=market_code, isin=isin, remarks=url
                )
                yield CompanyDataSourceItem(
                    company_id=company_code, company_name=name,
                    security_code=symbol,
                    download_link=(
                        self.financial_calendar_url + url.split('/')[-2]
                    )
                )

        # handle next page
        self.formdata['iDisplayStart'] = str(
            int(self.formdata['iDisplayStart']) + 20
        )
        yield scrapy.FormRequest(
            self.data_fetch_url,
            method='POST',
            formdata=self.formdata,
            callback=self.parse,
            errback=self.errback_scraping
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
