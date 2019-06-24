"""下载、更新公司英文列表"""

import re
import json
from json.decoder import JSONDecodeError

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyItem, ProfileDetailItem


class UpdateCompanyListSpider(scrapy.Spider):
    name = 'update_company_list_en'

    jdata_url = 'https://iss.moex.com/iss/apps/rms3/rates.json'
    jdata_referer = 'https://www.moex.com/s1859'

    formdata = {
        'instrumentgroups': (
            'stock_common_share,stock_preferred_share,'
            'stock_russian_depositary_receipt,stock_foreign_share_dr,'
            'stock_foreign_share'),
        'sort_column': 'SECID',
        'sort_order': 'asc',
        'lang': 'en',
        'start': '0'
    }

    # 字段映射表
    mate = {
        'SECID': 'security_code',
        'SHORTNAME': 'Short name',
        'NAME': 'name_en',
        'ISIN': 'isin',
        'TYPENAME': 'security_type',
        'LISTLEVEL': 'Listing level',
        'FACEUNIT': 'Face value of currency',
        'FACEVALUE': 'Face value',
        'ISSUESIZE': 'Issue Volume,units'
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
                select code, security_code from company where \
                country_code_listed='RUS'\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = {
            it['security_code']: it['code'] for it in records
        }

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

    def start_requests(self):
        yield scrapy.FormRequest(
            self.jdata_url,
            method='GET',
            formdata=self.formdata,
            callback=self.parse_companies,
            errback=self.errback_scraping,
        )

    def parse_companies(self, response):
        try:
            jres = json.loads(response.text)['rates']
            for each in jres['data']:  # 遍历结果项
                data = dict(zip(jres['columns'], each))
                if data['SECID'] not in self.companies:
                    self.max_code_num += 1
                    item = CompanyItem(
                        code='RUS' + str(self.max_code_num),
                    )
                    for key, val in data.items():
                        if key in self.mate:
                            try:
                                item[self.mate[key]] = val
                            except KeyError:
                                yield ProfileDetailItem(
                                    name=key.lower() + '_rus',
                                    display_label=self.mate[key],
                                    company_code=item['code'],
                                    value=val
                                )
                    yield item
                    self.total_new += 1

            if len(jres['data']) == 100:
                yield scrapy.Request(
                    re.sub(
                        r'(?<=start=)\d+', lambda m: str(int(m.group()) + 100),
                        response.request.url
                    ),
                    callback=self.parse_companies,
                    errback=self.errback_scraping
                )
        except JSONDecodeError:
            pass

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
