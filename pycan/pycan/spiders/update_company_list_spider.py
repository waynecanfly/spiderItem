"""下载、更新公司列表"""

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

from ..items import CompanyItem, ProfileDetailItem


class UpdateCompanyListSpider(scrapy.Spider):
    name = 'update_company_list'

    search_url = 'https://www.tsx.com/json/company-directory'
    profile_url = 'https://web.tmxmoney.com/company.php?qm_symbol={}&locale=en'

    start_urls = [
        'https://www.tsx.com/listings/listing-with-us/listed-company-directory'
    ]

    # 公司信息对应项
    pairs = {
        'Website:': ('./a/@href', 'website_url'),
        'Industry:': ('text()', 'industry'),
        'Sector:': ('text()', 'sector_code'),
        'CIK:': ('text()', 'info_disclosure_id')
    }

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
                select code, security_code, exchange_market_code, status from \
                company where country_code_listed='CAN'\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = {}
        for it in records:
            id_ = it['exchange_market_code'], it['security_code']
            self.companies[id_] = it['code'], it['status']

        if records:
            NUMBER = slice(3, None)  # 公司code数字编号区
            self.max_code_num = int(max(it['code'] for it in records)[NUMBER])
        else:
            self.max_code_num = 10000

        self.total_new = 0
        self.total_updated = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new, %d updated', spider.name,
            self.total_new, self.total_updated
        )

    def parse(self, response):
        statuses = response.xpath("//*[@id='nav-list']/li/a")
        exchanges = response.xpath(
            "//*[@id='choice-exchange']/input/@value").extract()
        letters = response.xpath(
            "//select/option[@value!='']/@value").extract()

        for status in statuses:
            action = status.xpath('@data-action').extract_first()
            label = status.xpath('text()').extract_first()
            if action == 'recent':
                continue
            for exchange in exchanges:
                dest_url = self.search_url + '/' + action + '/' + exchange
                if action == 'search':  # Currently Listed
                    for letter in letters:
                        yield scrapy.Request(
                            dest_url + '/' + letter,
                            callback=self.parse_companies,
                            errback=self.errback_scraping,
                            meta={'exchange': exchange, 'status': label}
                        )
                else:  # Recently Delisted and Suspended
                    yield scrapy.Request(
                        dest_url,
                        callback=self.parse_companies,
                        errback=self.errback_scraping,
                        meta={'exchange': exchange, 'status': label}
                    )

    def parse_companies(self, response):
        try:
            companies = json.loads(response.text)['results']
        except JSONDecodeError:
            companies = []

        exchange = response.meta['exchange'].upper()
        status = response.meta['status']
        for each in companies:
            id_ = (exchange, each['symbol'])
            if id_ not in self.companies:
                self.companies[id_] = status

                item = CompanyItem(
                    name_origin=each['name'], name_en=each['name'],
                    security_code=each['symbol'],
                    exchange_market_code=exchange, status=status
                )

                if 'instruments' in each:
                    item['instruments'] = json.dumps(each['instruments'])

                yield scrapy.Request(
                    self.profile_url.format(each['symbol']),
                    callback=self.parse_company_profile,
                    errback=self.errback_scraping,
                    meta={'item': item}
                )
            else:  # 判断公司状态是否有改变
                try:  # 每个交易所的一家公司均只有一个状态
                    company_code, company_status = self.companies[id_]
                    if status != company_status:
                        self.total_updated += 1
                        yield CompanyItem(
                            code=company_code, status=status,
                            gmt_update=datetime.now()
                        )
                except ValueError:
                    pass

    def parse_company_profile(self, response):
        item = response.meta['item']
        self.max_code_num += 1
        item['code'] = 'CAN' + str(self.max_code_num)

        self.total_new += 1

        if 'instruments' in item:
            yield ProfileDetailItem(
                name='instruments_can', display_label='instruments',
                data_type='string', company_code=item['code'],
                value=item['instruments']
            )

        if not response.css('.companyprofile>table'):
            yield item
        else:
            trs = response.css('.companyprofile>table>tr')
            for tr in trs:
                tds = tr.xpath('./td')
                for index, td in enumerate(tds):
                    label = td.xpath('text()').extract_first()
                    if label in self.pairs:
                        xpath_, field = self.pairs[label]
                        value = tds[index + 1].xpath(xpath_).extract_first()
                        if value:
                            item[field] = value
            yield item

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
