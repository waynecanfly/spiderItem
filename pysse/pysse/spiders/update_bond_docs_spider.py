"""下载、更新债券公告"""

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

from ..items import AnnounItem


class UpdateBondDocsSpider(scrapy.Spider):
    name = 'update_bond_docs'

    start_urls = [
        'http://bond.sse.com.cn/disclosure/announ/cb/'
    ]

    query_url = 'http://query.sse.com.cn/commonSoaQuery.do?'

    params = {
        'isPagination': True,
        'sqlId': 'BS_GGLL',
        'pageHelp.pageSize': 100,
        'siteId': 28,
        'channelId': 9862,
        'order': 'createTime|desc,stockcode|asc'
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateBondDocsSpider, cls).from_crawler(
            crawler, *args, **kwargs
        )
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        self.logger.info('Opening spider %s...', spider.name)
        conn = pymysql.connect(**self.settings['DBARGS'])
        sql = """\
            select max(disclosure_date) last_date from \
            securities_statement_index where user_create='lq'\
        """
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()

        if result['last_date']:
            self.last_date = str(result['last_date'])
        else:
            self.last_date = '2007-01-01'

    def spider_closed(self, spider):
        self.logger.info('Closing spider %s...', spider.name)

    def parse(self, response):
        options = response.xpath(
            "//*[@id='bulletinType']/option[not(@selected)]"
        )
        pairs = {}  # 公告类型表
        for each in options:
            id_ = each.xpath('@value').extract_first()
            type_ = each.xpath('text()').extract_first()
            pairs[id_] = type_

        req_url = self.query_url + urlencode(self.params)
        yield scrapy.Request(
            req_url,
            callback=self.parse_entries,
            errback=self.errback_scraping,
            meta={'req_url': req_url, 'pairs': pairs}
        )

    def parse_entries(self, response):
        if response.headers[b'Content-Type'].startswith(b'application/json'):
            jres = json.loads(response.text)
            page = jres['pageHelp']['pageNo']
            result = jres['result']
            if result:  # 通过结果集判断是否结束
                pairs = response.meta['pairs']
                for it in result:
                    if it['createTime'] >= self.last_date:
                        if it['extGGDL'] in pairs:
                            detail_type = pairs[it['extGGDL']]
                        else:
                            detail_type = it['extGGDL']
                        yield AnnounItem(
                            bond_code=it['stockcode'],
                            disclosure_date=it['createTime'],
                            doc_type=it['docType'],
                            doc_source_url='http://' + it['docURL'],
                            file_original_title=it['docTitle'],
                            detail_type=detail_type,
                            file_urls=['http://' + it['docURL']]
                        )
                    else:  # 通过createTime(disclosure_date)判断是否已下载
                        break
                else:  # 没有找到上次下载点，继续下一页
                    next_page = page + 1
                    self.params['pageHelp.pageNo'] = next_page
                    self.params['pageHelp.beginPage'] = next_page
                    self.params['pageHelp.endPage'] = next_page
                    req_url = self.query_url + urlencode(self.params)
                    yield scrapy.Request(
                        req_url,
                        callback=self.parse_entries,
                        errback=self.errback_scraping,
                        meta={'req_url': req_url, 'pairs': pairs},
                        headers={'Referer': self.start_urls[0]}
                    )
        else:
            self.logger.error(
                '%s has no json response', response.meta['req_url'])

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
