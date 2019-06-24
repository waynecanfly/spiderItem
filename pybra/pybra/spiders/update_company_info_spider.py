"""更新上市公司信息"""

import re
from collections import defaultdict
from itertools import groupby
from operator import itemgetter
from datetime import datetime

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyItem, ProfileDetailItem


class UpdateCompanyInfoSpider(scrapy.Spider):
    name = 'update_company_info'

    corporativos_url = (
        'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/'
        'ResumoEventosCorporativos.aspx?codigoCvm={}&tab=3&idioma=en-us'
    )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyInfoSpider, cls).from_crawler(
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
                select ta.code, ta.name_origin, ta.remarks as info_url, \
                tb.value, tc.name, tc.id as profile_id from company ta left \
                join (select * from company_profile_detail where company_code \
                like 'BRA%') tb on ta.code = tb.company_code left join \
                company_profile_definition tc on \
                tb.company_profile_definition_id = tc.id where \
                ta.country_code_listed = 'BRA' order by ta.code\
                """)
            records = cursor.fetchall()
        conn.close()

        self.companies = defaultdict(dict)
        for company, entries in groupby(records, itemgetter('code')):
            entries = list(entries)
            self.companies[company]['name'] = entries[0]['name_origin']
            self.companies[company]['info_url'] = entries[0]['info_url']
            self.companies[company]['profiles'] = {
                it['name']: it['value'] for it in entries
            }

        self.total_updated = 0
        self.total_unavailable = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d updated, %d unavailable',
            spider.name, self.total_updated, self.total_unavailable
        )

    def start_requests(self):
        for company, item in self.companies.items():
            req_url = item['info_url'] + '&idioma=en-us'
            yield scrapy.Request(
                req_url,
                callback=self.parse,
                errback=self.errback_scraping,
                meta={
                    'company': company,
                    'name': item['name'],
                    'profiles': item['profiles'],
                    'cvm_code': item['info_url'].split('=')[-1],
                    'req_url': req_url
                }
            )

    def parse(self, response):
        spans = response.xpath('//span')
        tips = spans.xpath('text()').extract_first()
        if len(spans) == 1 and tips == 'System unavailable.':
            self.total_unavailable += 1
            return

        company_name = response.xpath('//span[@id]/text()').extract_first()
        if company_name and company_name != response.meta['name']:  # 公司更名
            yield CompanyItem(
                code=response.meta['company'], name_origin=company_name,
                gmt_update=datetime.now()
            )
        overview_url = response.xpath('//iframe[@id]/@src').extract_first()
        if overview_url:
            yield response.follow(
                overview_url,
                callback=self.parse_overview,
                errback=self.errback_scraping,
                meta={
                    'company': response.meta['company'],
                    'cvm_code': response.meta['cvm_code'],
                    'profiles': response.meta['profiles'],
                    'req_url': response.meta['req_url']
                }
            )
        else:
            self.logger.warning(
                'System unavailable for %s on overview',
                response.meta['company']
            )

    def parse_overview(self, response):
        company = response.meta['company']
        profiles = response.meta['profiles']
        headers = ['Company Data', 'Contacts']
        updated = False

        is_available = True
        spans = response.xpath('//span')
        span_text = spans.xpath('text()').extract_first()
        ps = response.xpath('//p')
        p_text = ps.xpath('text()').extract_first()
        if len(spans) == 1 and span_text == 'System unavailable.':
            is_available = False
        if len(ps) == 1 and p_text == 'Data Unavailable.':
            is_available = False

        if not is_available:
            self.total_unavailable += 1
            yield scrapy.Request(
                self.corporativos_url.format(response.meta['cvm_code']),
                callback=self.parse_corporativos,
                errback=self.errback_scraping,
                headers={'Referer': response.meta['req_url']},
                meta={
                    'company': company,
                    'profiles': profiles,
                    'updated': updated
                }
            )
            return

        for each in headers:
            table = response.xpath(
                "//a[text()='{}']/following-sibling::div/table".format(each)
            )
            table = table[0]
            for row in table.xpath('./tr'):
                label = row.xpath(
                    './td[1]/text()').extract_first().strip(':')
                value = row.xpath(
                    'string(./td[2])').extract_first().strip()
                value = re.sub(r'\s+', ' ', value)
                profile_name = '_'.join(label.lower().split()) + '_bra'
                if profile_name not in profiles:  # 创建
                    yield ProfileDetailItem(
                        company_code=company, value=value,
                        gmt_create=datetime.now(),
                        company_profile_definition_id=profile_name,
                        parent_id='_'.join(each.lower().split()) + '_bra',
                        parent_label=each, label=label
                    )
                elif value != profiles[profile_name]:  # 修改
                    updated = True
                    yield ProfileDetailItem(
                        company_code=company, value=value,
                        gmt_update=datetime.now(),
                        company_profile_definition_id=profile_name
                    )

        yield scrapy.Request(
            self.corporativos_url.format(response.meta['cvm_code']),
            callback=self.parse_corporativos,
            errback=self.errback_scraping,
            headers={'Referer': response.meta['req_url']},
            meta={
                'company': company,
                'profiles': profiles,
                'updated': updated
            }
        )

    def parse_corporativos(self, response):
        spans = response.xpath('//span')
        if len(spans) == 1 and spans.xpath('text()') == 'System unavailable.':
            self.total_unavailable += 1
            return

        parent_label = 'Corporate Actions'
        profiles = response.meta['profiles']
        tables = response.xpath('//table')
        updated = False
        if tables:
            entries = tables[0].xpath('./tr')
            for it in entries:
                label = it.xpath(
                    './td[1]/span/text()').extract_first().strip(': ')
                value = it.xpath('./td[2]/span/text()').extract_first()
                value = value.strip() if value else value
                profile_name = '_'.join(label.lower().split()) + '_bra'
                if profile_name not in profiles:  # 创建
                    yield ProfileDetailItem(
                        company_code=response.meta['company'], value=value,
                        gmt_create=datetime.now(),
                        company_profile_definition_id=profile_name,
                        parent_id='corporate_actions_bra',
                        parent_label=parent_label, label=label
                    )
                elif value != profiles[profile_name]:  # 修改
                    updated = True
                    yield ProfileDetailItem(
                        company_code=response.meta['company'], value=value,
                        gmt_update=datetime.now(),
                        company_profile_definition_id=profile_name
                    )

        if response.meta['updated'] or updated:
            self.total_updated += 1

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
