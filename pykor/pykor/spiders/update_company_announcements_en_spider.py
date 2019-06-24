"""下载、更新公司英语非财报公告"""

import re
from itertools import groupby
from operator import itemgetter
from collections import defaultdict
from datetime import date, datetime

import scrapy
import pymysql
from scrapy import signals, FormRequest
from dateutil.parser import parse as parse_datetime
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import AnnounceItem


class UpdateCompanyAnnouncementsSpider(scrapy.Spider):

    name = 'update_company_announcements_en'

    formdata = {
        'method': 'searchDisclosureByCorpSub',
        'currentPageSize': '100',
        'pageIndex': '1',
        'orderMode': '1',
        'orderStat': 'D',
        'forward': 'searchdisclosurebycorp_sub',
        'searchCorpName': '',
        'fromDate': '2007-01-01',
        'toDate': str(date.today())
    }

    search_url = (
        'http://engkind.krx.co.kr/disclosure/searchdisclosurebycorp.do'
    )
    viewer_url = 'http://engkind.krx.co.kr/common/disclsviewer.do'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyAnnouncementsSpider, cls).from_crawler(
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
                select code, name_en, latest_date, language from \
                company left join (select company_code, \
                language_written_code language , max(disclosure_date) \
                latest_date from non_financial_statement_index where \
                country_code='KOR' group by company_code, language) t on \
                code = t.company_code where country_code_listed='KOR' \
                order by code\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = defaultdict(dict)
        for code, entries in groupby(records, itemgetter('code')):
            for each in entries:
                self.companies[code]['name'] = each['name_en']
                if each['language'] == 'en':
                    self.companies[code]['latest_date'] = each['latest_date']
                    break
            else:
                self.companies[code]['latest_date'] = datetime(2007, 1, 1)

        self.total_new = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new', spider.name, self.total_new
        )

    def start_requests(self):
        for it in self.companies:
            self.formdata['searchCorpName'] = self.companies[it]['name']
            yield FormRequest(
                self.search_url,
                formdata=self.formdata,
                callback=self.parse_entries,
                errback=self.errback_scraping,
                meta={
                    'code': it,
                    'latest_date': self.companies[it]['latest_date']
                }
            )

    def parse_entries(self, response):
        entries = response.xpath("//table/tbody/tr")
        for it in entries:
            disclosure_date = it.xpath('./td[2]/text()').extract_first()
            disclosure_date = parse_datetime(disclosure_date)
            if disclosure_date < response.meta['latest_date']:
                break
            try:
                title = it.xpath('./td[4]/a/text()').extract_first().strip()
                title = re.sub(r'\s+', ' ', title)
                doc_parts = it.xpath('./td[4]/a/@href').re(r"'(.*?)'")
                formdata = {
                    'method': 'search', 'acptNo': doc_parts[0],
                    'docno': doc_parts[1], 'formProcsCd': doc_parts[2],
                    'language': 'english'
                }
                yield FormRequest(
                    self.viewer_url,
                    method='GET',
                    formdata=formdata,
                    callback=self.search_contents,
                    errback=self.errback_scraping,
                    meta={
                        'code': response.meta['code'],
                        'disclosure_date': disclosure_date,
                        'title': title
                    }
                )
            except AttributeError:  # 一些标题项无相关文件及无信息的
                pass
        else:
            next_page = response.xpath(
                "//a[@class='next']/@onclick").re_first(r'(?<=\()\d+')
            if next_page:
                currenct_request = response.request
                current_request_body = currenct_request.body.decode()
                body = re.sub(
                    r'(?<=pageIndex=)\d+', next_page, current_request_body
                ).encode()
                yield currenct_request.replace(
                    body=body,
                )

    def search_contents(self, response):
        """通过可用docno获取相关文档链接"""
        docno = response.xpath(
            "//*[@id='mainDocId']/*[@selected]/@value").extract_first()
        if docno:
            yield FormRequest(
                self.viewer_url,
                method='GET',
                formdata={'method': 'searchContents', 'docNo': docno},
                callback=self.parse_docs_url,
                errback=self.errback_scraping,
                meta={
                    'code': response.meta['code'],
                    'disclosure_date': response.meta['disclosure_date'],
                    'title': response.meta['title']
                }
            )
        else:
            self.logger.error(
                'Failed searching docno for %s, %s, %s',
                response.meta['code'], response.meta['disclosure_date'],
                response.meta['title'])

    def parse_docs_url(self, response):
        catalog_url, doc_url, _ = re.findall(r"'(.*?)'", response.text)
        if not doc_url:
            self.logger.error(
                '%s, %s, %s has no attached doc.',
                response.meta['code'], response.meta['disclosure_date'],
                response.meta['title'])
        else:
            try:
                doc_type = doc_url.split('.')[-1]
            except IndexError:
                doc_type = 'unknow'
            yield AnnounceItem(
                company_code=response.meta['code'],
                fiscal_year=response.meta['disclosure_date'].year,
                disclosure_date=response.meta['disclosure_date'],
                doc_type=doc_type,
                doc_source_url=doc_url,
                file_original_title=response.meta['title'],
                file_urls=[doc_url]
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
