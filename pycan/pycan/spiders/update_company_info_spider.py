"""下载、更新公司列表"""

from collections import defaultdict
from itertools import groupby
from operator import itemgetter

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import ProfileDefinitionItem, ProfileDetailItem


class UpdateCompanyInfoSpider(scrapy.Spider):
    name = 'update_company_info'

    profile_url = 'https://web.tmxmoney.com/company.php?qm_symbol={}&locale=en'

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
                select code, security_code, value, name from company ta \
                left join (select company_profile_definition_id profile_id, \
                company_code, value from company_profile_detail where \
                company_code like 'CAN%') tb on ta.code = tb.company_code \
                left join company_profile_definition tc on \
                tb.profile_id = tc.id where country_code_listed='CAN' \
                order by code\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = defaultdict(dict)
        for company, entries in groupby(records, itemgetter('code')):
            entries = list(entries)
            self.companies[company]['symbol'] = entries[0]['security_code']
            self.companies[company]['profiles'] = {
                it['name']: it['value'] for it in entries
            }

        self.total_new = 0
        self.total_updated = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new, %d updated', spider.name,
            self.total_new, self.total_updated
        )

    def start_requests(self):
        for code in self.companies:
            symbol = self.companies[code]['symbol']
            yield scrapy.Request(
                self.profile_url.format(symbol),
                callback=self.parse_company_profile,
                errback=self.errback_scraping,
                meta={'company': code}
            )

    def parse_company_profile(self, response):
        entries = response.css('.data-table>tr')
        if entries:
            company = response.meta['company']
            profiles = self.companies[company]['profiles']
            pp_item = ProfileDefinitionItem()  # Parent Profile Item
            for each in entries:
                if each.css('.tableheader'):
                    display_label = each.css(
                        '.tableheader>th::text').extract_first()
                    pp_item['name'] = display_label.replace(' ', '_').lower() + '_can'
                    pp_item['display_label'] = display_label
                    pp_item['data_type'] = 'string'
                else:
                    labels = each.css('.label')
                    for it in labels:
                        display_label = it.xpath('text()').extract_first()
                        name = display_label.replace(' ', '_').lower() + '_can'
                        value = it.xpath(
                            'string(following-sibling::td)'
                        ).extract_first().strip()
                        if name not in profiles:  # 尚无该信息项，创建
                            self.total_new += 1
                            yield ProfileDetailItem(
                                name=name, display_label=display_label,
                                data_type='string', company_code=company,
                                value=value, parent=pp_item
                            )
                        elif value != profiles[name]:  # 信息有更新
                            self.total_updated += 1
                            yield ProfileDetailItem(
                                name=name, value=value, company_code=company
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
