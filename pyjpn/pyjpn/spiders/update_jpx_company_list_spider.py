"""Update company list of JPX(TOKYO STOCK EXCHANGE)"""

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyItem


class UpdateJPXCompanyListSpider(scrapy.Spider):
    name = 'update_jpx_company_list'

    start_urls = [
        'http://www2.tse.or.jp/tseHpFront/JJK020010Action.do?Show=Show'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateJPXCompanyListSpider, cls).from_crawler(
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
                select code, name_en, security_code from company where \
                country_code_listed='JPN' and user_create='lq'\
            """)
            records = cursor.fetchall()
        conn.close()

        self.symbols = [it['security_code'] for it in records]
        if records:
            NUMBER = slice(3, None)  # 公司code数字编号区
            self.max_code_num = int(max(it['code'] for it in records)[NUMBER])
        else:
            self.max_code_num = 10000

        self.total_new = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new companies', spider.name,
            self.total_new
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

        for row in range(2, len(entries), 3):
            code = entries[row].xpath('./td[1]/text()').extract_first().strip()
            name = entries[row + 1].xpath(
                './td[1]/text()').extract_first().strip()
            if code not in self.symbols:
                self.total_new += 1
                self.max_code_num += 1
                yield CompanyItem(
                    code='JPN' + str(self.max_code_num),
                    name_en=name,
                    security_code=code
                )

        if not response.css('.next_e'):  # it comes to the end
            return

        form_action = response.xpath('//form/@action').extract_first()
        hidden_names = response.xpath(
            "//*[@type='hidden' and not(@disabled)]/@name").extract()
        hidden_values = response.xpath(
            "//*[@type='hidden' and not(@disabled)]/@value").extract()
        formdata = dict(zip(hidden_names, hidden_values))
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
