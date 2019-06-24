"""下载、更新公司列表"""

from collections import defaultdict
from itertools import groupby
from operator import itemgetter
from datetime import datetime, date, timedelta

import scrapy
import pymysql
from scrapy import signals
from dateutil.parser import parse as parse_datetime
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import AnnounceItem, ReportItem


class UpdateCompanyDocsSpider(scrapy.Spider):
    name = 'update_company_docs'

    start_urls = [
        'https://www.sedar.com/search/search_form_pc_en.htm',
        'https://www.sedar.com/search/search_form_mf_en.htm'
    ]

    report_subjects = [
        'Annual Report', 'Financial Statements', 'Financial Statements - XBRL'
    ]

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
                select code, name_en, exchange_market_code, latest_date, \
                topic, name from company ta left join (select \
                max(disclosure_date) latest_date, \
                financial_statement_season_type_code topic, \
                company_code from financial_statement_index where \
                country_code='CAN' group by company_code, topic union select \
                max(disclosure_date) latest_date, \
                announcement_detail_type announce_type, company_code from \
                non_financial_statement_index where country_code='CAN' group \
                by company_code, announce_type) tb on ta.code=tb.company_code \
                left join non_financial_announcement_detail_type tc on \
                tb.topic=tc.id where ta.country_code_listed='CAN' \
                order by code\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = defaultdict(dict)
        for key, entries in groupby(records, itemgetter('code', 'name_en')):
            entries = list(entries)
            self.companies[key]['market'] = entries[0]['exchange_market_code']
            self.companies[key]['topics'] = {}
            for each in entries:
                if each['topic'] == 'FY':
                    self.companies[key]['topics'][
                        'Annual Report'] = each['latest_date']
                elif each['topic'] == 'Q':
                    self.companies[key]['topics'][
                        'Financial Statements'] = each['latest_date']
                    self.companies[key]['topics'][
                        'Financial Statements - XBRL'] = each['latest_date']
                else:
                    self.companies[key]['topics'][
                        each['name']] = each['latest_date']

        self.total_new = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new', spider.name, self.total_new
        )

    def parse(self, response):
        action = response.xpath('//form/@action').extract_first()
        hidden_names = response.xpath("//*[@type='hidden']/@name").extract()
        hidden_values = response.xpath("//*[@type='hidden']/@value").extract()
        formdata = dict(zip(hidden_names, hidden_values))
        formdata['industry_group'] = 'A'
        formdata['Variable'] = 'FilingDate'
        formdata['Search'] = 'Search'
        today = date.today()
        start = today - timedelta(weeks=1)
        from_year, from_month, from_date = str(start).split('-')
        to_year, to_month, to_date = str(today).split('-')
        '''
        formdata.update({
            'FromMonth': from_month,
            'FromDate': from_date,
            'FromYear': from_year,
            'ToMonth': to_month,
            'ToDate': to_date,
            'ToYear': to_year
        })
        '''
        formdata.update({
            'FromMonth': '01',
            'FromDate': '01',
            'FromYear': '2007',
            'ToMonth': to_month,
            'ToDate': to_date,
            'ToYear': to_year
        })
        subject_labels = response.xpath(
            "//*[@name='document_selection']/option[not(@selected)]/text()"
        ).extract()
        subject_values = response.xpath(
            "//*[@name='document_selection']/option[not(@selected)]/@value"
        ).extract()
        subjects = dict(zip(subject_labels, subject_values))

        for code, name in self.companies:
            if 'Fund' in name and 'mf' in response.url:
                pass
            elif 'Fund' not in name and 'pc' in response.url:
                pass
            else:
                continue
            formdata['company_search'] = name
            market = self.companies[(code, name)]['market']
            topics = self.companies[(code, name)]['topics']
            for subject, value in subjects.items():
                if subject not in topics:
                    topics[subject] = datetime(2007, 1, 1)
                formdata['document_selection'] = value
                req_url = response.urljoin(action)
                yield scrapy.FormRequest(
                    req_url,
                    formdata=formdata,
                    callback=self.parse_docs,
                    errback=self.errback_scraping,
                    meta={
                        'company': code,
                        'name': name,
                        'market': market,
                        'subject': subject,
                        'latest_date': topics[subject],
                        'req_url': req_url
                    }
                )

    def parse_docs(self, response):
        name = response.meta['name']
        try:
            data_table = response.xpath("//*[@id='content']//table")[1]
            entries = data_table.css('.rt')
        except IndexError:
            self.logger.error(
                'sedar error on %s, %s', response.meta['req_url'], name
            )
            return

        company = response.meta['company']
        market = response.meta['market']
        latest_date = response.meta['latest_date']
        subject = response.meta['subject']
        for it in entries:
            label_date = it.xpath('./td[2]/text() | ./td[3]/text()').extract()
            label_date = ' '.join(it.strip() for it in label_date)
            disclosure_date = parse_datetime(label_date)
            if disclosure_date < latest_date:
                break
            self.total_new += 1
            title = it.xpath('./td[4]//a/text()').extract_first()
            doc_action = it.xpath('./td[4]/form/@action').extract_first()
            doc_source_url = response.urljoin(doc_action)
            doc_type = it.xpath('./td[5]/text()').extract_first('').strip()
            item = AnnounceItem(
                exchange_market_code=market, company_code=company,
                disclosure_date=disclosure_date, doc_source_url=doc_source_url,
                doc_type=doc_type, file_original_title=title, subject=subject,
                file_urls=[doc_source_url],
            )
            if subject in self.report_subjects:
                yield ReportItem(item)
            else:
                yield item
        else:
            next_page = response.xpath(
                "//img[contains(@src, 'next')]/../@href").extract_first()
            if next_page:
                req_url = response.urljoin(next_page)
                yield scrapy.Request(
                    req_url,
                    callback=self.parse_docs,
                    errback=self.errback_scraping,
                    meta={
                        'latest_date': latest_date, 'subject': subject,
                        'market': market, 'company': company,
                        'req_url': req_url, 'name': name
                    }
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
