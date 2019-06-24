"""Update company list of JPX(TOKYO STOCK EXCHANGE)"""

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

from ..items import ProfileDetailItem


class UpdateJPXCompanyInfoSpider(scrapy.Spider):
    name = 'update_jpx_company_info'

    start_urls = [
        'http://www2.tse.or.jp/tseHpFront/JJK020010Action.do?Show=Show'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateJPXCompanyInfoSpider, cls).from_crawler(
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
                select ta.code, ta.security_code, tb.value, tc.id, tc.name \
                from company ta left join company_profile_detail tb on \
                ta.code = tb.company_code left join company_profile_definition\
                 tc on tb.company_profile_definition_id = tc.id where \
                ta.country_code_listed='JPN' and ta.security_code <> '' \
                order by code\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = defaultdict(dict)
        for symbol, entries in groupby(records, itemgetter('security_code')):
            entries = list(entries)
            self.companies[symbol]['code'] = entries[0]['code']
            self.companies[symbol]['profiles'] = {
                it['name']: it['value'] for it in entries
            }

        self.total_updated = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d updated', spider.name, self.total_updated
        )

    def parse(self, response):
        form_action = response.xpath("//form/@action").extract_first()
        if ';' in form_action:
            form_action = form_action.split(';')[0]
        if not form_action:
            return

        hidden_names = response.xpath(
            "//*[@type='hidden' and not(@disabled)]/@name").extract()
        hidden_values = response.xpath(
            "//*[@type='hidden' and not(@disabled)]/@value").extract()
        formdata = dict(zip(hidden_names, hidden_values))
        # set default value for select
        for it in response.xpath('//form//select'):
            name = it.xpath('@name').extract_first()
            value = it.xpath('./option[@selected]/@value').extract_first(' ')
            formdata[name] = value
        formdata['ListShow'] = 'ListShow'
        formdata['dspSsuPd'] = '200'  # 每页显示200
        # First Section, Second Section, Mothers, JASDAQ
        # First Section Foreign Stocks, Second Section Foreign Stocks
        # Mothers Foreign Stocks
        formdata['szkbuChkbx'] = [
            '001', '002', '004', '006', '101', '102', '104'
        ]

        yield scrapy.FormRequest(
            response.urljoin(form_action),
            method='POST',
            formdata=formdata,
            callback=self.parse_companies,
            errback=self.errback_scraping,
            meta={'page': 1}
        )

    def parse_companies(self, response):
        entries = response.xpath('//form/table/tr')
        if not entries:
            return

        # construct hidden data set
        form_action = response.xpath('//form/@action').extract_first()
        hidden_names = response.xpath(
            "//*[@type='hidden' and not(@disabled)]/@name").extract()
        hidden_values = response.xpath(
            "//*[@type='hidden' and not(@disabled)]/@value").extract()
        formdata = dict(zip(hidden_names, hidden_values))
        formdata['BaseJh'] = 'BaseJh'

        for row in range(2, len(entries), 3):
            code = entries[row].xpath('./td[1]/text()').extract_first().strip()
            mgrCd, jjHisiFlg = entries[row + 2].xpath(
                './/input/@onclick').re(r"'(.+?)',\s+'(.*?)'")
            formdata['mgrCd'] = mgrCd
            formdata['jjHisiFlg'] = jjHisiFlg
            if code in self.companies:
                yield scrapy.FormRequest(
                    response.urljoin(form_action),
                    method='POST',
                    formdata=formdata,
                    callback=self.parse_company_info,
                    errback=self.errback_scraping,
                    meta={'symbol': code}
                )

        if not response.css('.next_e'):  # it comes to the end
            return

        formdata.pop('BaseJh')
        formdata.pop('mgrCd')
        formdata.pop('jjHisiFlg')
        formdata['Transition'] = 'Transition'
        next_page = response.meta['page'] + 1
        formdata['lstDspPg'] = str(next_page)
        yield scrapy.FormRequest(
            response.urljoin(form_action),
            method='POST',
            formdata=formdata,
            callback=self.parse_companies,
            errback=self.errback_scraping,
            meta={'page': next_page}
        )

    def parse_company_info(self, response):
        profiles = self.companies[response.meta['symbol']]['profiles']
        company = self.companies[response.meta['symbol']]['code']

        tables = response.xpath('//table')
        tables_pos = [4, 5, 6]
        updated = False
        for pos in tables_pos:
            entries = tables[pos].xpath('./tr')
            for row in range(0, len(entries), 2):
                labels = [it.xpath('string()').extract_first().strip()
                          for it in entries[row].xpath('./th')]
                names = [it.lower().replace(' ', '_') + '_jpx_jpn'
                         for it in labels]
                values = [it.xpath('string()').extract_first().strip()
                          for it in entries[row + 1].xpath('./td')]

                for index, name in enumerate(names):
                    if name not in profiles:  # create
                        updated = True
                        yield ProfileDetailItem(
                            company_code=company, label=labels[index],
                            label_name=name, value=values[index],
                            gmt_create=datetime.now(), user_create='lq'
                        )
                    elif values[index] != profiles[name]:  # update
                        updated = True
                        yield ProfileDetailItem(
                            company_code=company, value=values[index],
                            label_name=name, gmt_update=datetime.now(),
                            user_update='lq'
                        )

        if updated:
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
