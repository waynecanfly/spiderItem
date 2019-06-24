"""下载、更新公司韩文公司信息"""

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

from ..items import CompanyItem, ProfileDefinitionItem
from ..items import ProfileDetailItem
from ..utils.mate import profile_abbrs_ko, business_ko


class UpdateCompanyInfoSpider(scrapy.Spider):
    name = 'update_company_info_ko'

    start_urls = [(
        'http://marketdata.krx.co.kr/contents/MKD/04/0402/04020100/'
        'MKD04020100T2.jsp')]

    autocomplete_url = (
        'http://marketdata.krx.co.kr/WEB-APP/autocomplete/autocomplete.jspx')
    otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'

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
                select code, name_origin from company where \
                country_code_listed='KOR'\
            """)
            self.companies = cursor.fetchall()
        conn.close()

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s...', spider.name,
        )

    def parse(self, response):
        """请求公司自动补全信息，保存data-bld用于请求表单data-code"""
        action = response.xpath('//form[@action]/@action').extract_first()
        data_bld = response.xpath('//form[@action]/@data-bld').extract_first()
        page_path = response.xpath(
            "//input[@name='pagePath']/@value").extract_first()

        for it in self.companies:
            yield FormRequest(
                self.autocomplete_url,
                method='GET',
                formdata={
                    'contextName': 'stkisu3', 'value': it['name_origin'],
                    'bldPath': '/COM/finder_stkisu_autocomplete',
                    'viewCount': '5',
                },
                callback=self.parse_comapny_data,
                errback=self.errback_scraping,
                meta={
                    'code': it['code'], 'name': it['name_origin'],
                    'data_bld': data_bld, 'action': action,
                    'page_path': page_path
                }
            )

    def parse_comapny_data(self, response):
        """解析自动补全的公司信息项，并请求用于后续处理的data_code"""
        for it in response.xpath('//li'):
            data_tp = it.xpath('@data-tp').extract_first()
            data_nm = it.xpath('@data-nm').extract_first()
            data_cd = it.xpath('@data-cd').extract_first()

            if data_nm == response.meta['name']:
                data_bld = response.meta['data_bld'][:-1] + '2'
                yield FormRequest(
                    self.otp_url,
                    method='GET',
                    formdata={'bld': data_bld, 'name': 'tablesubmit'},
                    callback=self.search_company,
                    errback=self.errback_scraping,
                    meta={
                        'code': response.meta['code'], 'key': 'profie',
                        'action': response.meta['action'], 'data_tp': data_tp,
                        'data_nm': data_nm, 'data_cd': data_cd,
                        'page_path': response.meta['page_path'],
                        'bld': data_bld}
                )
                data_bld = response.meta['data_bld'][:-1] + '3'
                yield FormRequest(
                    self.otp_url,
                    method='GET',
                    formdata={'bld': data_bld[:-1] + '3', 'name': 'div'},
                    callback=self.search_company,
                    errback=self.errback_scraping,
                    meta={
                        'code': response.meta['code'], 'key': 'business',
                        'action': response.meta['action'], 'data_tp': data_tp,
                        'data_nm': data_nm, 'data_cd': data_cd,
                        'page_path': response.meta['page_path']}
                )
                break
        else:
            self.logger.error(
                'unexpected autocomplete for %s', response.meta['name'])

    def search_company(self, response):
        """基于请求的data-code及其它信息搜索公司"""
        req_url = response.urljoin(response.meta['action'])
        data_tp = response.meta['data_tp']
        data_nm = response.meta['data_nm']
        formdata = {
            'isu_cdnm': '{}/{}'.format(data_tp, data_nm),
            'isu_cd': response.meta['data_cd'],
            'isu_nm': data_nm,
            'isu_srt_cd': data_tp,
            'pagePath': response.meta['page_path'],
            'code': response.text,
        }
        key = response.meta['key']
        if key == 'business':
            yield FormRequest(
                req_url,
                formdata=formdata,
                callback=self.parse_company_info,
                errback=self.errback_scraping,
                meta={'code': response.meta['code'], 'key': key}
            )
        else:
            formdata['bldcode'] = response.meta['bld']
            yield FormRequest(
                req_url,
                formdata=formdata,
                callback=self.parse_company_info,
                errback=self.errback_scraping,
                meta={'code': response.meta['code'], 'key': key}
            )

    def parse_company_info(self, response):
        try:
            data = json.loads(response.text)['DS1']
        except JSONDecodeError:
            return

        if response.meta['key'] == 'profie':  # 公司概况
            data = data[0]
            item = CompanyItem(
                code=response.meta['code'], website_url=data['home_pg'],
                ipo_date=data['lst_dt'], established_date=data['estb_dt']
            )
            yield item  # 公司基本信息

            profile_item = ProfileDefinitionItem(
                name=profile_abbrs_ko['parent'][0],
                display_label=profile_abbrs_ko['parent'][1],
                data_type='string'
            )
            for key, val in profile_abbrs_ko.items():  # 公司详情信息
                try:
                    yield ProfileDetailItem(
                        name=key + '_ko_kor', display_label=val,
                        company_code=response.meta['code'], value=data[key],
                        parent=profile_item
                    )
                except KeyError:
                    pass
        else:  # 公司业务内容
            html = ''
            for each in data:
                html += each['ctnt']
            yield ProfileDetailItem(
                name=business_ko['name'],
                display_label=business_ko['display_label'],
                company_code=response.meta['code'], value=html,
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
