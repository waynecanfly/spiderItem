"""Update non reports in edinet"""

from collections import defaultdict
from itertools import groupby
from operator import itemgetter
from urllib.parse import urlencode
from datetime import datetime

import scrapy
import pymysql
from scrapy import signals
from dateutil.parser import parse as parse_datetime
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import AnnounceItem


class UpdateEdinetCompanyAnnouncesSpider(scrapy.Spider):
    name = 'update_edinet_company_announces'

    types_ = {
        'lpr': 'Report of Possession of Large Volume',
        'oth': 'Other types of documents'
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateEdinetCompanyAnnouncesSpider, cls).from_crawler(
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
                select ta.company_id, ta.download_link, tb.latest_date, \
                tc.name from company_data_source ta left join (select \
                company_code, announcement_detail_type, max(disclosure_date) \
                latest_date from non_financial_statement_index where \
                country_code='JPN' group by company_code, \
                announcement_detail_type) tb on ta.company_id = \
                tb.company_code left join \
                non_financial_announcement_detail_type tc on \
                tb.announcement_detail_type = tc.id where \
                company_id like 'JPN%' order by company_id\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = defaultdict(dict)
        for company, entries in groupby(records, itemgetter('company_id')):
            entries = list(entries)
            self.companies[company]['url'] = entries[0]['download_link']
            self.companies[company]['docs'] = {
                it['name']: it['latest_date'] for it in entries
            }

        self.total_docs = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d docs.', spider.name,
            self.total_docs
        )

    def start_requests(self):
        for code in self.companies:
            docs_url = self.companies[code]['url']
            docs = self.companies[code]['docs']
            for type_ in self.types_:
                type_name = self.types_[type_]
                if type_name in docs:
                    latest_date = docs[type_name]
                else:
                    latest_date = datetime(2007, 1, 1)
                yield scrapy.Request(
                    docs_url.replace('fls=', type_ + '='),
                    callback=self.parse,
                    errback=self.errback_scraping,
                    meta={
                        'company': code,
                        'type_name': type_name,
                        'latest_date': latest_date
                    }
                )

    def parse(self, response):
        entries = response.xpath(
            "//table[contains(@class, 'result')]/tr[position()>1]")
        if not entries:
            return

        PID = response.xpath("//input[@name='PID']/@value").extract_first()
        req_args = {
            'uji.verb': '',
            'uji.bean': '',
            'PID': PID
        }
        for it in entries:
            disclosure_date = it.xpath(
                './td[1]/div/text()').extract_first().strip()
            disclosure_date = parse_datetime(disclosure_date)
            if disclosure_date <= response.meta['latest_date']:
                break
            title = it.xpath('string(./td[2])').extract_first().strip()
            pdf_href = it.xpath('./td[6]//a/@href').extract_first()
            pdf_url = response.urljoin(pdf_href) if pdf_href else pdf_href
            xbrl_parts = it.xpath('./td[7]//a/@onclick').re(r"\((.*?)\)")
            if xbrl_parts:
                verb, bean, pid, tid, other, url = xbrl_parts[0].replace(
                    "'", '').replace(' ', '').split(',')
                req_args['uji.verb'] = verb
                req_args['uji.bean'] = bean
                download_url = response.urljoin(url)
                if '?' in download_url:
                    download_url = download_url.split('?')[0] + '?'
                xbrl_url = download_url + urlencode(req_args) + '&' + other
                yield AnnounceItem(
                    company_code=response.meta['company'],
                    disclosure_date=disclosure_date, doc_type='zip',
                    doc_source_url=xbrl_url, file_original_title=title,
                    announcement_detail_type=response.meta['type_name'],
                    file_urls=[xbrl_url]
                )
            else:
                yield AnnounceItem(
                    company_code=response.meta['company'],
                    disclosure_date=disclosure_date, doc_type='pdf',
                    doc_source_url=pdf_url, file_original_title=title,
                    announcement_detail_type=response.meta['type_name'],
                    file_urls=[pdf_url]
                )
            self.total_docs += 1

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
