"""更新edinet上市公司信息"""

import csv
from io import BytesIO
from zipfile import ZipFile
from codecs import iterdecode
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

from ..items import CompanyItem, ProfileDetailItem


class UpdateEdinetCompanyInfoSpider(scrapy.Spider):
    name = 'update_edinet_company_info'

    start_urls = [(
        'https://disclosure.edinet-fsa.go.jp/E01EW/download?uji.verb='
        'W1E62071EdinetCodeDownload&uji.bean=ee.bean.W1E62071.EEW1E62071Bean&'
        'TID=W1E62072&PID=W1E62072&lgKbn=1&dflg=0&iflg=0&dispKbn=1'
    )]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateEdinetCompanyInfoSpider, cls).from_crawler(
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
                select ta.code, ta.info_disclosure_id edinet_code, ta.status, \
                tb.value, ta.security_code, tc.id, tc.name from company ta \
                left join company_profile_detail tb on \
                ta.code = tb.company_code \
                left join company_profile_definition tc on \
                tb.company_profile_definition_id = tc.id where \
                ta.country_code_listed='JPN' order by code\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = defaultdict(dict)
        for company, entries in groupby(records, itemgetter('code')):
            entries = list(entries)
            self.companies[company]['symbol'] = entries[0]['security_code']
            self.companies[company]['status'] = entries[0]['status']
            self.companies[company]['edinet_code'] = entries[0]['edinet_code']
            self.companies[company]['profiles'] = {
                it['name']: it['value'] for it in entries
            }

        self.total_updated = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d updated', spider.name, self.total_updated
        )

    def parse(self, response):
        if b'Content-Disposition' in response.headers:
            with ZipFile(BytesIO(response.body)) as myzip:
                with myzip.open('EdinetcodeDlInfo.csv') as myfile:
                    fcsv = csv.reader(iterdecode(myfile, 'CP932'))
                    entries = list(fcsv)

            updated = False
            labels = entries[1]
            name_suffix = '_edinet_jpn'
            names = [x.lower().replace(' ', '_') + name_suffix for x in labels]

            for item in entries[2:]:
                edinet_code = item[0]
                for company in self.companies:
                    if self.companies[company]['edinet_code'] == edinet_code:
                        # update company info: status, security_code
                        status = self.companies[company]['status']
                        symbol = self.companies[company]['symbol']
                        if (symbol != item[11] or
                                not item[2].startswith(status)):
                            updated = True
                            if item[2]:
                                new_status = item[2].split()[0]
                            else:
                                new_status = item[2]
                            yield CompanyItem(
                                code=company, name_origin=item[6],
                                name_en=item[7], security_code=item[11],
                                status=new_status,
                                gmt_update=datetime.now(), user_update='lq'
                            )
                        # create or update profile
                        profiles = self.companies[company]['profiles']
                        for index, name in enumerate(names):
                            if name not in profiles:  # 创建
                                updated = True
                                yield ProfileDetailItem(
                                    company_code=company, value=item[index],
                                    label_name=name, label=labels[index],
                                    gmt_create=datetime.now(), user_create='lq'
                                )
                            elif item[index] != profiles[name]:  # 更新
                                updated = True
                                yield ProfileDetailItem(
                                    company_code=company, value=item[index],
                                    label_name=name, gmt_update=datetime.now(),
                                    user_update='lq'
                                )
                        break

                if updated:
                    self.total_updated += 1

    def errback_scraping(self, failure):
        req_url = failure.request.url
        if failure.check(HttpError):
            response = failure.value.response
            if response.status != 404:
                self.logger.error(
                    'HttpError %s on %s', response.status, req_url)
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
