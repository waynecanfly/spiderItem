"""全量更新上交所证券信息"""

import json
from urllib.parse import urlencode

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import BondItem
from ..utils.mate import fields_mappings


class UpdateBondInfoMonthlySpider(scrapy.Spider):
    name = 'update_bond_info_monthly'

    info_url = 'http://bond.sse.com.cn/disclosure/info/index.shtml?BONDCODE='
    query_url = 'http://query.sse.com.cn/commonQuery.do?'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateBondInfoMonthlySpider, cls).from_crawler(
            crawler, *args, **kwargs
        )
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        self.logger.info('Opening spider %s...', spider.name)
        conn = pymysql.connect(**self.settings['DBARGS'])
        with conn.cursor() as cursor:
            cursor.execute(
                "select * from bond_of_china where user_create='lq'"
            )
            self.records = cursor.fetchall()
        conn.close()

        self.updated_bonds = 0

    def spider_closed(self, spider):
        self.logger.info('Updated %s bonds', self.updated_bonds)
        self.logger.info('Closing spider %s...', spider.name)

    def start_requests(self):
        params = {
            'isPagination': False,
            'sqlId': 'COMMON_BOND_BASIC_ZQJBYS_L',
            'BONDCODE': None
        }

        for it in self.records:
            params['BONDCODE'] = it['bond_code']
            req_url = self.query_url + urlencode(params)
            yield scrapy.Request(
                req_url,
                callback=self.parse,
                errback=self.errback_scraping,
                headers={'Referer': self.info_url + it['bond_code']},
                meta={'req_url': req_url, 'db_item': it}
            )

    def parse(self, response):
        req_url = response.meta['req_url']
        if response.headers[b'Content-Type'].startswith(b'application/json'):
            db_item = response.meta['db_item']
            data = json.loads(response.text)['result'][0]
            bond_item = BondItem()
            for field, id_ in fields_mappings.items():
                bond_item[field] = data[id_]

            item = BondItem()
            for field, val in bond_item.items():
                if val != db_item[field]:  # 债券信息有更新
                    item[field] = val

            if len(item):
                self.updated_bonds += 1
                item['bond_code'] = db_item['bond_code']
                yield item
        else:
            self.logger.error('%s has no json response.', req_url)

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
