"""下载国家及其下辖政区信息"""

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..utils.dbman import query_sql
from ..items import CountryItem, RegionItem, IndicatorItem


class IndicatorsIndexSpider(scrapy.Spider):

    name = 'indicators_index'

    start_urls = [
        'https://knoema.com/atlas'
    ]

    sqls = {
        'country': 'select code, name from macro_data_country',
        'region': (
            'select ta.code country, tb.name from macro_data_country ta right '
            'join macro_data_region tb on ta.id = tb.country_id'
        ),
        'indicator': (
            'select lower(ta.name) parent_name, lower(tb.name) name, tb.level '
            'from macro_data_indicator ta right join macro_data_indicator tb '
            'on ta.id = tb.parent_id'
        )
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(IndicatorsIndexSpider, cls).from_crawler(
            crawler, *args, **kwargs
        )
        crawler.signals.connect(spider.spider_opened, signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self, spider):
        self.logger.info('Opening spider %s...', spider.name)
        conn = pymysql.connect(**self.settings['DBARGS'])
        with conn.cursor() as cursor:
            result = query_sql(cursor, self.sqls['country'])
            self.countries = [(it['code'], it['name']) for it in result]
            result = query_sql(cursor, self.sqls['region'])
            self.regions = [(it['country'], it['name']) for it in result]
            result = query_sql(cursor, self.sqls['indicator'])
            self.indicators = [
                (it['name'], it['level'], it['parent_name']) for it in result
            ]
        conn.close()

    def spider_closed(self, spider):
        self.logger.info('Closing spider %s...', spider.name)

    def parse(self, response):
        """按国家搜索数据"""
        entries = response.xpath("//*[@class='container']/ul//a")
        for it in entries:
            href = it.xpath('@href').extract_first()
            name = it.xpath('text()').extract_first()
            if href and name:
                url = response.urljoin(href)
                yield scrapy.Request(
                    url,
                    callback=self.parse_country_profile,
                    errback=self.errback_scraping,
                    meta={'country': name, 'url': url}
                )

    def parse_country_profile(self, response):
        """解析国家基本信息并搜索相关指标信息"""
        flag_href = response.xpath(
            "//img[@class='flag']/@src").extract_first()
        country_code = flag_href.split('/')[-1].split('.')[0].upper()

        profiles = {}
        entries = response.xpath("//*[@class='facts']/ul/li[boolean(./span)]")
        for it in entries:
            key = it.xpath('./span/text()').extract_first().strip(': ')
            val = it.xpath('string()').extract_first().strip()[len(key) + 1:]
            profiles[key] = val

        # 尚未存储该国家信息
        if (country_code, response.meta['country']) not in self.countries:
            yield CountryItem(
                code=country_code, name=response.meta['country'],
                url=response.meta['url'], profiles=profiles
            )

        # 搜索国家下辖行政区域信息
        regions_url = response.xpath(
            "//*[@class='regions']//a/@href").extract_first()
        if regions_url:
            yield scrapy.Request(
                regions_url,
                callback=self.search_regions,
                errback=self.errback_scraping,
                meta={'country': country_code}
            )

        # 指标信息搜索
        entries = response.xpath("//*[@class='indicator-column']/li")
        for each in entries:
            name = each.xpath('./h3/text()').extract_first()
            url = each.xpath('./a/@href').extract_first()  # see-more-topics
            if name and url:
                indicator = (name.strip().lower(), 1, None)  # 一级指标
                if indicator not in self.indicators:  # 尚未存储该指标信息
                    self.indicators.append(indicator)
                    yield IndicatorItem(
                        name=name.strip(), level=1, parent_id=None
                    )
                yield scrapy.Request(
                    url,
                    callback=self.parse_indicators,
                    errback=self.errback_scraping,
                    meta={'name': name},
                )

    def search_regions(self, response):
        """搜索各行政区域信息"""
        entries = response.xpath("//*[@class='container']/ul//a")
        for it in entries:
            url = it.xpath('@href').extract_first()
            name = it.xpath('text()').extract_first()
            yield scrapy.Request(
                url,
                callback=self.parse_region_profiles,
                errback=self.errback_scraping,
                meta={
                    'name': name, 'country': response.meta['country'],
                    'url': url
                }
            )

    def parse_indicators(self, response):
        """解析通过more方式获取的指标信息"""
        parent_name = response.meta['name'].strip().lower()
        entries = response.xpath("//*[@class='two-column']/li")
        for it in entries:
            title = it.xpath('./h3/text()').extract_first()  # 二级指标名称
            if not title:
                continue
            indicator = (title.strip().lower(), 2, parent_name)  # 二级指标信息
            if indicator not in self.indicators:
                self.indicators.append(indicator)
                yield IndicatorItem(
                    name=title.strip(), level=2,
                    parent_id=response.meta['name'].strip()
                )
            for each in it.xpath('./ul/li'):
                name = each.xpath('./a/text()').extract_first()  # 三级指标名称
                indicator = (name.strip().lower(), 3, title.strip().lower())
                if indicator not in self.indicators:
                    self.indicators.append(indicator)
                    yield IndicatorItem(
                        name=name.strip(), level=3, parent_id=title.strip())

    def parse_region_profiles(self, response):
        """解析行政区域信息及相关指标信息"""
        profiles = {}
        entries = response.xpath("//*[@class='facts']/ul/li[boolean(./span)]")
        for it in entries:
            key = it.xpath('./span/text()').extract_first().strip(': ')
            val = it.xpath('string()').extract_first().strip()[len(key) + 1:]
            profiles[key] = val

        country_code = response.meta['country']
        if (country_code, response.meta['name']) not in self.regions:
            yield RegionItem(
                code=response.meta['url'].split('/')[-1],
                name=response.meta['name'], url=response.meta['url'],
                profiles=profiles, country_id=country_code
            )

        entries = response.xpath("//*[@class='indicator-column']/li")
        for each in entries:
            title = each.xpath('./h3/text()').extract_first()  # 一级指标名称
            if not title:
                continue
            indicator = (title.strip().lower(), 1, None)  # 一级指标信息
            if indicator not in self.indicators:
                self.indicators.append(indicator)
                yield IndicatorItem(
                    name=title.strip(), level=1, parent_id=None)
            if each.xpath("./a[@class='see-more-topics']"):  # see-more-topics
                yield scrapy.Request(
                    each.xpath('./a/@href').extract_first(),
                    callback=self.parse_indicators,
                    errback=self.errback_scraping,
                    meta={'name': title}
                )
            else:
                for every in each.xpath('./ul/li'):
                    name = every.xpath('./a/text()').extract_first()
                    indicator = (
                        name.strip().lower(), 2, title.strip().lower())
                    if indicator not in self.indicators:
                        self.indicators.append(indicator)
                        yield IndicatorItem(
                            name=name.strip(), level=2,
                            parent_id=title.strip())

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
