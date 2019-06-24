"""更新公司详情信息（英语）"""

import re
import json
from collections import defaultdict
from itertools import groupby
from operator import itemgetter
from json.decoder import JSONDecodeError

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import ProfileDetailItem


class UpdateCompanyInfoSpider(scrapy.Spider):
    name = 'update_company_info_en'

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
        'SHORTNAME': 'Short name',
        'LISTLEVEL': 'Listing level',
        'FACEUNIT': 'Face value of currency',
        'FACEVALUE': 'Face value',
        'ISSUESIZE': 'Issue Volume,units'
    }

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
                company_code like 'RUS%') tb on ta.code = tb.company_code \
                left join company_profile_definition tc on \
                tb.profile_id = tc.id where country_code_listed='RUS' \
                order by security_code\
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

        self.total_new = 0
        self.total_updated = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %s new, %s updated', spider.name,
            self.total_new, self.total_updated
        )

    def start_requests(self):
        yield scrapy.FormRequest(
            self.jdata_url,
            method='GET',
            formdata=self.formdata,
            headers={'Referer': self.jdata_referer},
            callback=self.parse_companies,
            errback=self.errback_scraping,
        )

    def parse_companies(self, response):
        try:
            jres = json.loads(response.text)['rates']
            for each in jres['data']:  # 遍历结果项
                data = dict(zip(jres['columns'], each))
                if data['SECID'] in self.companies:
                    company = self.companies[data['SECID']]
                else:
                    continue
                for name, display_label in self.mate.items():
                    p_name = name.lower() + '_rus'
                    if p_name not in company['profiles']:  # create
                        self.total_new += 1
                        yield ProfileDetailItem(
                            name=p_name, display_label=display_label,
                            company_code=company['code'], value=data[name],
                        )
                    elif str(data[name]) != str(company['profiles'][p_name]):
                        self.total_updated += 1
                        yield ProfileDetailItem(
                            name=p_name, value=data[name],
                            company_code=company['code']
                        )

            if len(jres['data']) == 100:  # 翻页
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
