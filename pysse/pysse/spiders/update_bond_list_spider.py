"""增量下载上交所证券信息"""

import re
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


class UpdateBondListSpider(scrapy.Spider):
    name = 'update_bond_list'

    start_urls = [
        'http://bond.sse.com.cn/disclosure/info/tb/allBonds.js'
    ]
    code_pat = re.compile(r"\['(\d+)'\].*?,'(\w+)'")
    code_type = '公司债券'
    info_url = 'http://bond.sse.com.cn/disclosure/info/index.shtml?BONDCODE='
    query_url = 'http://query.sse.com.cn/commonQuery.do?'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateBondListSpider, cls).from_crawler(
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
                select bond_code from bond_of_china where user_create='lq'\
            """)
            records = cursor.fetchall()
        self.db_codes = [it['bond_code'] for it in records]
        conn.close()

        self.new_bonds = 0  # 记录新下载证券数

    def spider_closed(self, spider):
        self.logger.info('Fetched %s new bonds', self.new_bonds)
        self.logger.info('Closing spider %s...', spider.name)

    def parse(self, response):
        params = {
            'isPagination': False,
            'sqlId': 'COMMON_BOND_BASIC_ZQJBYS_L',
            'BONDCODE': None
        }

        total_bonds = 0
        lines = response.text.split(';')
        for line in lines:
            if re.search(self.code_pat, line):
                code, type_ = re.search(self.code_pat, line).groups()
                if type_ == self.code_type:
                    total_bonds += 1
                    if code not in self.db_codes:
                        self.new_bonds += 1
                        params['BONDCODE'] = code
                        req_url = self.query_url + urlencode(params)
                        yield scrapy.Request(
                            req_url,
                            callback=self.parse_info,
                            errback=self.errback_scraping,
                            meta={'req_url': req_url}
                        )

    def parse_info(self, response):
        req_url = response.meta['req_url']
        if response.headers[b'Content-Type'].startswith(b'application/json'):
            data = json.loads(response.text)['result'][0]
            item = BondItem()
            for field, id_ in fields_mappings.items():
                item[field] = data[id_]
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
