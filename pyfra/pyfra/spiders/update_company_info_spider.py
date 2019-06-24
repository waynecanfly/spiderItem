import re
from urllib.parse import urlencode
from collections import defaultdict
from itertools import groupby
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

from ..items import CompanyItem, ProfileDefinitionItem, ProfileDetailItem


class UpdateCompanyInfoSpider(scrapy.Spider):
    name = 'update_company_info'

    market_url = 'https://www.euronext.com/en/factsheet-ajax'
    market_params = {'instrument_id': '', 'instrument_type': 'equities'}

    profile_url = 'https://www.euronext.com/en/nyx-company-profile/ajax'

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
                select code, remarks, value, name, security_type, sector_code,\
                industry, ipo_date, currency_code, website_url from company \
                ta left join (select * from company_profile_detail where \
                company_code like 'FRA%') tb on ta.code = tb.company_code \
                left join company_profile_definition tc on \
                tb.company_profile_definition_id = tc.id where \
                country_code_listed='FRA' and remarks is not null \
                order by code\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = defaultdict(dict)
        for company, entries in groupby(records, itemgetter('code')):
            entries = list(entries)
            self.companies[company]['type'] = entries[0]['security_type']
            self.companies[company]['sector'] = entries[0]['sector_code']
            self.companies[company]['industry'] = entries[0]['industry']
            self.companies[company]['ipo_date'] = entries[0]['ipo_date']
            self.companies[company]['currency'] = entries[0]['currency_code']
            self.companies[company]['website'] = entries[0]['website_url']
            self.companies[company]['url'] = entries[0]['remarks']
            self.companies[company]['profiles'] = {
                it['name']: it['value'] for it in entries
            }

        self.total_update = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d updated', spider.name, self.total_update)

    def start_requests(self):
        for company in self.companies:
            instrument_id = self.companies[company]['url'].split('/')[-2]
            self.market_params['instrument_id'] = instrument_id
            yield scrapy.Request(
                self.market_url + '?' + urlencode(self.market_params),
                callback=self.parse_market_information,
                errback=self.errback_scraping,
                meta={'code': company}
            )

            yield scrapy.Request(
                self.profile_url + '?instrument_id=' + instrument_id,
                callback=self.parse_company_information,
                errback=self.errback_scraping,
                meta={'code': company}
            )

    def parse_market_information(self, response):
        captions = response.xpath('//h3')
        if not captions:
            return

        company = self.companies[response.meta['code']]
        profiles = company['profiles']
        item = CompanyItem(code=response.meta['code'])

        security_type = response.xpath(
            "//*[contains(text(), 'Instrument Type')]/strong/text()"
        ).extract_first('')
        if security_type != company['type']:
            item['security_type'] = security_type

        market_item = ProfileDefinitionItem(
            name='market_information_fra',
            display_label='Market Information', data_type='string')
        for it in captions[1:]:
            title = it.xpath('text()').extract_first()
            parent_item = ProfileDefinitionItem(
                display_label=title, parent=market_item, data_type='string',
                name=title.lower().replace(' ', '_') + '_fra',
            )
            entries = it.xpath('following-sibling::div')
            for entry in entries:
                label = entry.xpath('./span/text()').extract_first('').strip()
                val = entry.xpath('./strong/text()').extract_first('').strip()
                if label == 'Industry':
                    try:
                        industry = val.split(',')[0]
                        if industry != company['industry']:
                            item['industry'] = industry
                    except IndexError:
                        pass
                elif label == 'Sector':
                    try:
                        sector = val.split(',')[0]
                        if sector != company['sector']:
                            item['sector_code'] = sector
                    except IndexError:
                        pass
                elif label == 'Trading currency' and val:
                    if val != company['currency']:
                        item['currency_code'] = val
                elif label == 'IPO date' and val:
                    if company['ipo_date'] != parse_datetime(val):
                        item['ipo_date'] = parse_datetime(val)

                if label == '-':
                    pass
                else:
                    label_name = label.lower().replace(' ', '_') + '_fra'
                    if label_name not in profiles:
                        yield ProfileDetailItem(
                            display_label=label, name=label_name,
                            company_code=response.meta['code'], value=val,
                            parent=parent_item, data_type='string'
                        )
                    elif val != profiles[label_name]:
                        self.total_update += 1
                        yield ProfileDetailItem(
                            name=label_name, value=val,
                            company_code=response.meta['code'],
                        )
        if len(item) > 1:
            self.total_update += 1
            yield item

    def parse_company_information(self, response):
        entries = response.css('.header')
        if not entries:
            return

        company = self.companies[response.meta['code']]
        profiles = company['profiles']

        profile_item = ProfileDefinitionItem(
            name='company_information_fra',
            display_label='Company Information', data_type='string')
        for it in entries:
            title = it.xpath('text()').extract_first()
            content = it.xpath('following-sibling::div[1]')
            if title == 'Key Figures':
                continue
            parent_item = ProfileDefinitionItem(
                display_label=title, parent=profile_item,
                name=title.lower().replace(' ', '_') + '_fra',
                data_type='string'
            )
            if content.xpath('.//table'):
                rows = content.xpath('.//table/tbody/tr')
                for row in rows:
                    label = row.xpath('./td[1]/text()').extract_first()
                    val = row.xpath('./td[2]/text()').extract_first()
                    label_name = label.lower().replace(' ', '_') + '_fra'
                    if label_name not in profiles:
                        yield ProfileDetailItem(
                            display_label=label, name=label_name, value=val,
                            parent=parent_item, data_type='string',
                            company_code=response.meta['code']
                        )
                    elif val != profiles[label_name]:
                        self.total_update += 1
                        yield ProfileDetailItem(
                            name=label_name, value=val,
                            company_code=response.meta['code']
                        )
            else:
                label_name = title.lower().replace(' ', '_') + '_fra'
                vals = []
                for each in content.css('.address-group'):
                    val = each.xpath('string()').extract_first().strip()
                    vals.append(val)
                if vals:
                    value = '|'.join(vals)
                    if label_name not in profiles:
                        yield ProfileDetailItem(
                            display_label=title, name=label_name,
                            parent=profile_item, value=value,
                            company_code=response.meta['code'],
                            data_type='string'
                        )
                    elif value != profiles[label_name]:
                        self.total_update += 1
                        yield ProfileDetailItem(
                            name=label_name, parent=profile_item,
                            value=value, company_code=response.meta['code']
                        )

                    if re.search(r'http.*\.\w{2,3}', value):
                        url = re.search(r'http.*\.\w{2,3}', value).group()
                        if url != company['website']:
                            yield CompanyItem(
                                code=response.meta['code'],
                                website_url=url
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
