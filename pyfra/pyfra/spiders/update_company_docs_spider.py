from datetime import datetime
from operator import itemgetter

import scrapy
import pymysql
from scrapy import signals
from dateutil.parser import parse as parse_datetime
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import AnnounceItem, ReportItem, CompanyDataSourceItem


class UpdateCompanyDocsSpider(scrapy.Spider):
    name = 'update_company_docs'

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
                select ta.code, ta.exchange_market_code, tb.download_link, \
                tb.latest_date, tc.latest_announ_date from company ta join \
                company_data_source tb on ta.code = tb.company_id left join \
                (select company_code, max(disclosure_date) latest_announ_date \
                from non_financial_statement_index where country_code='FRA' \
                group by company_code) tc on ta.code = tc.company_code where \
                ta.country_code_listed='FRA' and tb.download_link is not null\
            """)
            self.companies = cursor.fetchall()

        self.total_docs = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d docs.', spider.name, self.total_docs
        )

    def start_requests(self):
        for it in self.companies[:]:
            if isinstance(it['latest_announ_date'], datetime):
                latest_announ_date = it['latest_announ_date']
            else:
                latest_announ_date = datetime(2007, 1, 1)
            yield scrapy.Request(
                it['download_link'],
                callback=self.parse,
                errback=self.errback_scraping,
                meta={
                    'company': it['code'],
                    'market': it['exchange_market_code'],
                    'latest_date': it['latest_date'],
                    'latest_announ_date': latest_announ_date
                }
            )

    def parse(self, response):
        latest_date = response.meta['latest_date']
        latest_announ_date = response.meta['latest_announ_date']
        company = {
            'code': response.meta['company'], 'market': response.meta['market']
        }

        entries = response.xpath(
            "//*[@id='nyx-financial-calendar-details']/div")
        stop_reports, stop_announs = False, False
        for index, each in enumerate(entries):
            try:
                fiscal_year = each.xpath('@id').re(r'\d{4}')[0]
            except IndexError:
                continue
            tables = each.xpath('./div/table')
            if not stop_reports:
                report_items = self.parse_report_items(tables[0], company)
                for pos, it in enumerate(report_items):
                    if (index == 0 and pos == 0 and
                            it['disclosure_date'] > latest_date):
                        yield CompanyDataSourceItem(
                            company_id=company['code'],
                            latest_url=it['doc_source_url'],
                            latest_date=it['disclosure_date']
                        )
                    if it['disclosure_date'] > latest_date:
                        it['fiscal_year'] = fiscal_year
                        self.total_docs += 1
                        yield it
                    else:
                        stop_reports = True
            if not stop_announs:
                announ_items = self.parse_announ_items(tables[1:], company)
                for it in announ_items:
                    if it['disclosure_date'] > latest_announ_date:
                        it['fiscal_year'] = fiscal_year
                        self.total_docs += 1
                        yield it
                    else:
                        stop_announs = True
            if stop_reports and stop_announs:
                break

    def parse_report_items(self, table, company):
        items = []
        caption = table.xpath('./caption/text()').extract_first()
        trs = table.xpath('./tr')
        for index, tr in enumerate(trs):
            if tr.css('.expand-btn'):
                title = tr.xpath('./td[1]/text()').extract_first()
                title_date = tr.xpath('./td[2]/text()').extract_first()
                try:
                    pair = trs[index + 1]
                except IndexError:
                    continue
                doc_label = pair.xpath('.//tr/td[1]/text()').extract_first()
                doc_url = pair.xpath('.//tr/td[2]/a/@href').extract_first()

                try:
                    disclosure_date = parse_datetime(title_date)
                except TypeError:
                    try:
                        disclosure_date = parse_datetime(title)
                        title = doc_label
                    except TypeError:
                        disclosure_date = None

                if disclosure_date:
                    if '.' in doc_url:
                        doc_type = doc_url.split('.')[-1]
                    else:
                        doc_type = 'unknow'
                    items.append(ReportItem(
                        company_code=company['code'],
                        exchange_market_code=company['market'],
                        disclosure_date=disclosure_date, doc_type=doc_type,
                        doc_source_url=doc_url, caption=caption,
                        file_original_title=title, file_urls=[doc_url],
                    ))
        items.sort(key=itemgetter('disclosure_date'), reverse=True)
        return items

    def parse_announ_items(self, tables, company):
        result = []
        for table in tables:
            items = self.parse_report_items(table, company)
            result.extend(items)
        result.sort(key=itemgetter('disclosure_date'), reverse=True)
        for index, it in enumerate(result):
            result[index] = AnnounceItem(it)
        return result

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
