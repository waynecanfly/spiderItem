"""从sec更新美国上市公司信息"""

from itertools import groupby
from operator import itemgetter
from collections import defaultdict
from datetime import datetime

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import ProfileDetailItem, CompanyItem
from ..utils.mate import profiles_mapping


class UpdateCompanyInfoSpider(scrapy.Spider):
    name = 'update_company_info'

    rss_feed_url = (
        "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}"
        "&type=&dateb=&owner=exclude&start=0&count=40&output=atom"
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
                select ta.code, ta.name_en, ta.info_disclosure_id as cik, \
                tb.value, tc.name, tc.id as profile_id from company ta left \
                join (select * from company_profile_detail where company_code \
                like 'USA%') tb on ta.code = tb.company_code left join \
                company_profile_definition tc on \
                tb.company_profile_definition_id = tc.id where \
                ta.country_code_listed = 'USA'\
                """)
            records = cursor.fetchall()
            self.companies = defaultdict(dict)
            for company, entries in groupby(records, itemgetter('code')):
                entries = list(entries)
                self.companies[company]['name'] = entries[0]['name_en']
                self.companies[company]['cik'] = entries[0]['cik']
                self.companies[company]['profiles'] = {
                    it['name']: it['value'] for it in entries
                }
        conn.close()

        self.total_renamed = 0  # 更名公司
        self.total_updated = 0  # 公司信息有更新的

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %s renamed, %s updated', spider.name,
            self.total_renamed, self.total_updated
        )

    def start_requests(self):
        for code, item in self.companies.items():
            yield scrapy.Request(
                self.rss_feed_url.format(item['cik']),
                callback=self.parse,
                errback=self.errback_scraping,
                meta={
                    'code': code,
                    'name': item['name'],
                    'profiles': item['profiles']
                }
            )

    def parse(self, response):
        company_code = response.meta['code']
        company_name = response.meta['name']
        profiles = response.meta['profiles']

        is_updated = False
        response.selector.register_namespace(
            'ns', 'http://www.w3.org/2005/Atom'
        )
        entries = response.xpath('//ns:company-info/*')
        for each in entries:
            label = each.xpath('name()').extract_first()  # 使用节点名称作键
            if each.xpath('./*'):  # addresses和formerly-names
                formerly_vals = []
                for index, child in enumerate(each.xpath('./*'), 1):
                    parts = []
                    type_ = child.xpath('@type').extract_first(
                        default=str(index)
                    )
                    key = label + '_' + type_  # 复合键
                    for part in child.xpath('./*'):
                        name = part.xpath('name()').extract_first()
                        val = part.xpath('text()').extract_first()
                        parts.append(name + ':' + str(val))  # 存在空值节点
                    if key.startswith('formerly'):  # 合并所有曾用名
                        formerly_vals.append(';'.join(parts))
                    else:
                        profile_name = key + '_usa'  # 地址信息逐个比对
                        address_vals = ';'.join(parts)
                        if profile_name in profiles:
                            if profiles[profile_name] != address_vals:
                                is_updated = True
                                yield ProfileDetailItem(
                                    company_profile_definition_id=key,
                                    company_code=company_code,
                                    value=address_vals, user_update='lq'
                                )
                        else:
                            yield ProfileDetailItem(
                                company_profile_definition_id=key,
                                company_code=company_code, value=address_vals,
                                user_create='lq'
                            )
                if key.startswith('formerly'):  # 曾用名信息合并比较
                    formerly_names = '$'.join(formerly_vals)
                    if 'formerly_usa' in profiles:  # 公司已有曾用名信息
                        if profiles['formerly_usa'] != formerly_names:
                            is_updated = True
                            yield ProfileDetailItem(
                                company_profile_definition_id='formerly',
                                company_code=company_code,
                                value=formerly_names, user_update='lq',
                                gmt_update=datetime.now()
                            )
                    else:  # 首次出现曾用名信息
                        yield ProfileDetailItem(
                            company_profile_definition_id='formerly',
                            company_code=company_code,
                            value=formerly_names, user_create='lq',
                        )
            else:
                value = each.xpath('text()').extract_first()
                if label == 'conformed-name' and value != company_name:  # 公司改名
                    self.total_renamed += 1
                    yield CompanyItem(
                        code=company_code, name_origin=value, name_en=value,
                        gmt_update=datetime.now(), user_update='lq'
                    )
                elif label in profiles_mapping:
                    profile_name = label + '_usa'
                    if profile_name in profiles:
                        if profiles[profile_name] != value:  # 公司信息有更新
                            is_updated = True
                            yield ProfileDetailItem(
                                company_profile_definition_id=label,
                                company_code=company_code, value=value,
                                user_update='lq', gmt_update=datetime.now()
                            )
                    else:  # 初次获取公司信息
                        yield ProfileDetailItem(
                            company_profile_definition_id=label,
                            company_code=company_code, value=value,
                            user_create='lq'
                        )

        if is_updated:
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
