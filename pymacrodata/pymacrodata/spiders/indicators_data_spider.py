"""下载指标数据"""

import re
import json
from json.decoder import JSONDecodeError

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..utils.dbman import query_sql
from ..items import IndicatorDataItem


class IndicatorDataSpider(scrapy.Spider):

    name = 'indicators_data'

    data_pat = re.compile(r'(?<=ld\+json">).*?(?=</)', re.DOTALL)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(IndicatorDataSpider, cls).from_crawler(
            crawler, *args, **kwargs
        )
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        self.logger.info('Opening spider %s...', spider.name)
        conn = pymysql.connect(**self.settings['DBARGS'])
        with conn.cursor() as cursor:
            self.countries = query_sql(
                cursor, 'select id, code, url from macro_data_country')
        conn.close()

    def spider_closed(self, spider):
        self.logger.info('Closing spider %s...', spider.name)

    def start_requests(self):
        for it in self.countries:
            yield scrapy.Request(
                it['url'],
                callback=self.parse,
                errback=self.errback_scraping,
                meta={'country_id': it['id'], 'country_code': it['code']}
            )

    def parse(self, response):
        """通过'更多'进入指标详情页"""
        indicators = response.xpath("//*[@class='indicator-column']/li")
        for each in indicators:
            name = each.xpath('./h3/text()').extract_first()
            url = each.xpath('./a/@href').extract_first()
            if name and url:
                yield scrapy.Request(
                    url,
                    callback=self.query_indicators_data,
                    errback=self.errback_scraping,
                    meta={
                        'name': name,
                        'country_id': response.meta['country_id'],
                        'country_code': response.meta['country_code']
                    },
                )

    def query_indicators_data(self, response):
        """查询指标数据"""
        entries = response.xpath("//*[@class='two-column']/li")
        for it in entries:
            title = it.xpath('./h3/text()').extract_first()
            if not title:
                continue
            for each in it.xpath('./ul/li'):
                name = each.xpath('./a/text()').extract_first()
                url = each.xpath('./a/@href').extract_first()
                yield scrapy.Request(
                    url,
                    callback=self.parse_indicators_data,
                    errback=self.errback_scraping,
                    meta={
                        'country_code': response.meta['country_code'],
                        'country_id': response.meta['country_id'],
                        'indicators': [3, name.strip(), title.strip()],
                        'url': url
                    }
                )

    def parse_indicators_data(self, response):
        """json-ld数据解析（待研究）"""
        try:
            data = re.search(self.data_pat, response.text).group()
            jdata = json.loads(data)
            entries = jdata[
                '@graph'][0]['mainEntity']['csvw:tableSchema']['csvw:columns']
            result = []
            for each in entries:
                title = each['csvw:name']
                values = [it['csvw:value'] for it in each['csvw:cells']]
                values.insert(0, title)
                for i in range(len(values)):
                    values[i] = values[i].replace(
                        '\xa0', ' ').replace(',', '|#|')
                result.append(values)
            yield IndicatorDataItem(
                country_code=response.meta['country_code'],
                country_id=response.meta['country_id'],
                indicator_id=response.meta['indicators'],
                url=response.meta['url'], data=zip(*result)
            )
        except (AttributeError, JSONDecodeError, KeyError):
            pass

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
