import re

import scrapy
import pymysql
from scrapy import signals
from dateutil.parser import parse as parse_datetime
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import AnnounceItem, CompanyDataSourceItem


class UpdateCompanyDocsSpider(scrapy.Spider):
    name = 'update_company_docs'

    rns_pattern = re.compile(r"loadBoxContent\('(.+?)','(.+?)'")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyDocsSpider, cls).from_crawler(
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
                select company_id, download_link, latest_url, latest_date \
                from company_data_source where company_id like 'GBR%' and \
                user_create='lq'\
                """)
            self.entries = cursor.fetchall()
        conn.close()

        self.total_docs = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d reports.', spider.name,
            self.total_docs
        )

    def start_requests(self):
        for it in self.entries:
            yield scrapy.Request(
                it['download_link'],
                callback=self.parse,
                errback=self.errback_scraping,
                meta={
                    'company': it['company_id'],
                    'latest_url': it['latest_url'],
                    'latest_date': it['latest_date']
                }
            )

    def parse(self, response):
        news_href = response.xpath("//a[@title='News']/@href").extract_first()
        if news_href:
            yield response.follow(
                news_href,
                dont_filter=True,
                callback=self.start_rns,
                errback=self.errback_scraping,
                meta={
                    'company': response.meta['company'],
                    'latest_url': response.meta['latest_url'],
                    'latest_date': response.meta['latest_date']
                }
            )

    def start_rns(self, response):
        rns_match = re.search(self.rns_pattern, response.text)
        if rns_match:
            title, url = rns_match.groups()
            if title == 'rns-wrapper':
                yield response.follow(
                    url,
                    callback=self.parse_rns,
                    errback=self.errback_scraping,
                    meta={
                        'company': response.meta['company'],
                        'latest_url': response.meta['latest_url'],
                        'latest_date': response.meta['latest_date'],
                        'first': True  # 第一页标识项
                    }
                )

    def parse_rns(self, response):
        latest_url = response.meta['latest_url']
        latest_date = response.meta['latest_date']

        entries = response.css('.newsContainer')

        if 'first' in response.meta and entries:  # 记录本次最新下载点
            doc_url, _, _ = entries[0].xpath('./a/@href').re(r"'(.+?)'")
            doc_url = response.urljoin(doc_url)
            disclosure_date = entries[0].css('.hour::text').extract_first('')
            disclosure_date = parse_datetime(disclosure_date)
            if latest_url != doc_url and disclosure_date >= latest_date:
                yield CompanyDataSourceItem(
                    company_id=response.meta['company'],
                    latest_url=doc_url, latest_date=disclosure_date,
                )

        for it in entries:
            doc_url, _, _ = it.xpath('./a/@href').re(r"'(.+?)'")
            doc_url = response.urljoin(doc_url)
            disclosure_date = it.css('.hour::text').extract_first('')
            disclosure_date = parse_datetime(disclosure_date)
            if disclosure_date >= latest_date:
                if latest_url != doc_url:
                    self.total_docs += 1
                    title = it.xpath('./a/text()').extract_first('').strip()
                    yield AnnounceItem(
                        company_code=response.meta['company'],
                        fiscal_year=disclosure_date.year,
                        disclosure_date=disclosure_date,
                        file_original_title=title,
                        doc_type=doc_url.split('.')[-1],
                        doc_source_url=doc_url,
                        file_urls=[doc_url]
                    )
            else:
                break
        else:
            next_info = response.xpath("//*[@title='Next']").re(r"'(.+?)'")
            if next_info:
                _, next_page = next_info
                yield response.follow(
                    next_page,
                    callback=self.parse_rns,
                    errback=self.errback_scraping,
                    meta={
                        'company': response.meta['company'],
                        'latest_url': latest_url,
                        'latest_date': latest_date
                    }
                )

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
