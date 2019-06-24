"""下载、更新公司英文列表"""

import html
import json
from json import JSONDecodeError

import scrapy
import pymysql
from scrapy import FormRequest
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyItem, ProfileDetailItem


class UpdateCompanyListSpider(scrapy.Spider):
    name = 'update_company_list_en'

    start_urls = [(
        'http://global.krx.co.kr/contents/GLB/05/0503/0503010100/'
        'GLB0503010100.jsp')]

    otp_url = 'http://global.krx.co.kr/contents/COM/GenerateOTP.jspx'

    fields = {
        'name_en': 'kor_cor_nm',
        'security_code': 'isu_cd',
        'industry': 'std_ind_cd',
        'currency_code': 'iso_cd'
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyListSpider, cls).from_crawler(
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
                select code, security_code, name_en from company where \
                country_code_listed='KOR'\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = {}
        for it in records:
            self.companies[it['security_code']] = it['code'], it['name_en']

        if records:
            NUMBER = slice(3, None)  # 公司code数字编号区
            self.max_code_num = int(max(it['code'] for it in records)[NUMBER])
        else:
            self.max_code_num = 10000

        self.total_new = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new', spider.name, self.total_new
        )

    def parse(self, response):
        """基于market，通过data-bld信息获取用于搜索公司所需的code值"""
        markets = {
            'KOSPI': 'STK',
            'KOSDAQ': 'KSQ',
            'KONEX': 'KNX'
        }
        bld = response.xpath('//form/@data-bld').extract_first()
        for market, value in markets.items():
            request = FormRequest.from_response(
                response,
                formdata={'market_gubun': value},
                formxpath='//form[@action]',
                callback=self.parse_companies,
                errback=self.errback_scraping,
                meta={'market': market}
            )
            yield FormRequest(
                self.otp_url,
                method='GET',
                formdata={'bld': bld, 'name': 'form'},
                callback=self.search_companies,
                errback=self.errback_scraping,
                meta={'request': request}
            )

    def search_companies(self, response):
        """查询公司"""
        data_code = response.text
        request = response.meta['request']
        req = request.replace(
            body=request.body + '&code={}'.format(data_code).encode()
        )
        yield req

    def parse_companies(self, response):
        """解析公司信息，生成新的公司项"""
        try:
            companies = json.loads(response.text)['block1']
            for each in companies:
                item = CompanyItem({
                    name: each[field] for name, field in self.fields.items()
                })
                item['name_en'] = html.unescape(item['name_en'])
                if item['security_code'] not in self.companies:
                    self.max_code_num += 1
                    item['code'] = 'KOR' + str(self.max_code_num)
                    self.companies[item['security_code']] = (
                        item['code'], item['name_en'])
                    self.total_new += 1
                    yield item
                    yield ProfileDetailItem(
                        name='market_kor', display_label='Market',
                        data_type='string', company_code=item['code'],
                        value=response.meta['market']
                    )
                else:  # 公司是否重命名的处理待定
                    pass
        except JSONDecodeError:
            self.logger.error(
                '%s got unexpected response.', response.meta['market'])

    def errback_scraping(self, failure):
        """对于post请求的处理缺乏细节"""
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
