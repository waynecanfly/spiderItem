"""更新edinet上市公司列表"""

import re
import csv
from io import BytesIO
from zipfile import ZipFile
from codecs import iterdecode
from urllib.parse import urlencode

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyItem, CompanyDataSourceItem


class UpdateEdinetCompanyListSpider(scrapy.Spider):
    name = 'update_edinet_company_list'

    start_urls = ['http://disclosure.edinet-fsa.go.jp']

    frm_action = (
        'https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp'
    )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateEdinetCompanyListSpider, cls).from_crawler(
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
                select code, info_disclosure_id from company where \
                user_create='lq' and country_code_listed='JPN'\
                """)
            records = cursor.fetchall()
        conn.close()

        self.edinet_codes = [it['info_disclosure_id'] for it in records]
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
        """To english page"""
        eng_href = response.xpath(
            "//img[@alt='English']/parent::a/@href").extract_first()
        if eng_href:
            query_str = '?lgKbn=1&dflg=0&iflg=0'
            eng_url = response.urljoin(eng_href) + query_str
            yield response.follow(
                eng_url,
                callback=self.parse_list_page,
                errback=self.errback_scraping,
                meta={'eng_url': eng_url}
            )

    def parse_list_page(self, response):
        formdata = {
            'uji.verb': '',
            'uji.bean': '',
            'PID': '',
            'TID': '',
            'SESSIONKEY': '',
            'lgKbn': '1',
            'dflg': '0',
            'iflg': '0'
        }
        list_parts = response.xpath(
            "//a[contains(string(), '& Code List')]/@href").re(r"'(.*?)'")

        # 用于构造company_data_source表中的download_link
        doc_parts = response.xpath(
            "//img[@alt='Document Search']/parent::a/@href").re(r"'(.*?)'")
        if doc_parts and list_parts:
            formdata['uji.verb'] = doc_parts[0]
            formdata['uji.bean'] = doc_parts[1]
            formdata['PID'] = doc_parts[2]
            formdata['TID'] = doc_parts[3]
            doc_search_url = self.frm_action + '?' + urlencode(formdata)

            verb, bean, pid, tid, other = list_parts
            formdata['uji.verb'] = verb
            formdata['uji.bean'] = bean
            formdata['PID'] = pid
            formdata['TID'] = tid
            yield scrapy.FormRequest(
                self.frm_action,
                method='GET',
                formdata=formdata,
                callback=self.start_code_list,
                errback=self.errback_scraping,
                meta={
                    'doc_search_url': doc_search_url,
                    'eng_url': response.meta['eng_url']
                }
            )

    def start_code_list(self, response):
        func_parts = response.xpath(
            "//td[text()='EDINET Code List']/following-sibling::td//a/@href"
        ).re(r":(.*)(?=\().+'(.*?)'")
        if func_parts:
            hidden_names = response.xpath(
                "//input[@type='hidden']/@name").extract()
            hidden_values = response.xpath(
                "//input[@type='hidden']/@value").extract()
            formdata = dict(zip(hidden_names, hidden_values))

            func_name, params = func_parts
            args_match = re.search(
                r"function\s+%s.*?downloadFile\((.*?)\)" % func_name,
                response.text, re.DOTALL
            )
            if args_match:
                args = args_match.group(1)
                verb, bean, pid, tid, url = re.findall(r"'(.*?)'", args)
                formdata['uji.verb'] = verb
                formdata['uji.bean'] = bean
                formdata['PID'] = pid
                formdata['TID'] = tid

                for each in params.split('&'):
                    key, value = each.split('=')
                    formdata[key] = value

                form_url = response.urljoin(url)
                yield scrapy.FormRequest(
                    form_url,
                    formdata=formdata,
                    method='GET',
                    callback=self.parse_companies,
                    errback=self.errback_scraping,
                    meta={
                        'doc_search_url': response.meta['doc_search_url'],
                        'eng_url': response.meta['eng_url']
                    }
                )

    def parse_companies(self, response):
        if b'Content-Disposition' in response.headers:
            companies = []
            with ZipFile(BytesIO(response.body)) as myzip:
                with myzip.open('EdinetcodeDlInfo.csv') as myfile:
                    fcsv = csv.reader(iterdecode(myfile, 'CP932'))
                    for row_num, item in enumerate(fcsv, 1):
                        if row_num > 2:
                            if item[2] and item[0] not in self.edinet_codes:
                                self.max_code_num += 1
                                companies.append(CompanyItem(
                                    code='JPN' + str(self.max_code_num),
                                    name_origin=item[6],
                                    name_en=item[7],
                                    security_code=item[11],
                                    industry=item[10],
                                    status=item[2].split()[0],
                                    info_disclosure_id=item[0],
                                ))

            yield scrapy.Request(
                response.meta['doc_search_url'],
                callback=self.start_doc_search,
                errback=self.errback_scraping,
                headers={'Referer': response.meta['eng_url']},
                meta={'companies': companies}
            )

    def start_doc_search(self, response):
        """拼凑company_data_source表中的download_link"""
        func_name = response.xpath(
            "//input[contains(@value, 'Search')]/@onclick").re(r":(.*)(?=\()")
        if func_name:
            hidden_names = response.xpath(
                "//input[@type='hidden']/@name").extract()
            hidden_values = response.xpath(
                "//input[@type='hidden']/@value").extract()
            formdata = dict(zip(hidden_names, hidden_values))

            func_name = func_name[0]
            args_match = re.search(
                r"function\s+%s.*?doAction\((.*?)\)" % func_name,
                response.text, re.DOTALL
            )
            if args_match:
                args = args_match.group(1)
                verb, bean, pid, tid = re.findall(r'"(.*?)"', args)
                formdata['uji.verb'] = verb
                formdata['uji.bean'] = bean
                formdata['PID'] = pid
                formdata['TID'] = tid
                formdata['fls'] = 'on'
                formdata['pfs'] = 5

                for it in response.meta['companies']:
                    self.total_new += 1
                    formdata['mul'] = it['info_disclosure_id']
                    docs_link = self.frm_action + '?' + urlencode(formdata)
                    yield it  # CompanyItem
                    yield CompanyDataSourceItem(
                        company_id=it['code'], company_name=it['name_origin'],
                        security_code=it['security_code'],
                        download_link=docs_link,
                        info_disclosure_id=it['info_disclosure_id']
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
