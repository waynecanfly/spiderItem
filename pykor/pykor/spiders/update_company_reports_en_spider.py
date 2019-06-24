"""下载、更新公司英语财报文档"""

import re
from datetime import date, datetime

import scrapy
import pymysql
from scrapy import signals, FormRequest
from dateutil.parser import parse as parse_datetime
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import ReportItem


class UpdateCompanyReportsSpider(scrapy.Spider):

    name = 'update_company_reports_en'

    start_urls = [
        'http://englishdart.fss.or.kr/corp/searchCorpEngL.ax'
    ]

    search_index = 'http://englishdart.fss.or.kr/dsbd002/main.do'
    search_corp = 'http://englishdart.fss.or.kr/corp/searchCorpEngA.ax'
    search_docs = 'http://englishdart.fss.or.kr/dsbd002/search.ax'

    doc_download = (
        'http://englishdart.fss.or.kr/pdf/download/main_eng.do?'
        'rcp_no={}&dcm_id={}&dcm_seq={}')

    formdata = {
        'currentPage': '1',
        'textCrpCik': '',
        'textCrpNm': '',
        'startDate': '20070101',
        'endDate': str(date.today()).replace('-', ''),
        'closingAccounts': ['0401', '0403', '0402', '0404']
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyReportsSpider, cls).from_crawler(
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
                select code, security_code, latest_date from company left \
                join (select company_code, max(disclosure_date) latest_date \
                from financial_statement_index where country_code='KOR' \
                group by company_code) t on code = company_code where \
                country_code_listed='KOR'\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = {}
        for it in records:
            if it['latest_date']:
                self.companies[it['security_code']] = (
                    it['code'], it['latest_date'])
            else:
                self.companies[it['security_code']] = (
                    it['code'], datetime(2007, 1, 1))
        self.total_new = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new', spider.name, self.total_new
        )

    def parse(self, response):
        """通过公司搜索页查找待用公司信息"""
        for i in range(26):  # 对应字母表搜索公司
            form_req = FormRequest.from_response(
                response,
                self.search_corp,
                formdata={'searchIndex': str(i)},
                headers={'Referer': self.search_index},
                callback=self.parse_companies,
                errback=self.errback_scraping,
                meta={'seq': i, 'first': True, 'current': 1},
            )
            yield form_req.replace(url=self.search_corp)

    def parse_companies(self, response):
        """解析公司信息获取搜索公司报表所需额外参数"""
        entries = response.xpath("//*[contains(@summary, 'content')]/tbody/tr")
        for it in entries:
            code = it.xpath('./td[3]/text()').extract_first().strip()
            if code in self.companies:
                self.formdata['textCrpCik'] = it.xpath(
                    './td[1]/input/@value').extract_first()
                self.formdata['textCrpNm'] = it.xpath(
                    './td[1]/span/input/@value').extract_first()
                yield FormRequest(
                    self.search_docs,
                    formdata=self.formdata,
                    callback=self.parse_docs_info,
                    errback=self.errback_scraping,
                    headers={'Referer': self.search_index},
                    meta={
                        'company': self.companies[code][0], 'first': True,
                        'current': 1, 'latest_date': self.companies[code][1]
                    }
                )

        if 'first' in response.meta:
            try:
                last = int(response.xpath(
                    "//*[@href='#last']/@onclick").re_first(r'(?<=\()\d+'))
            except TypeError:
                last = 0
        else:
            last = response.meta['last']
        if response.meta['current'] < last:
            currenct_request = response.request
            current_request_body = currenct_request.body.decode()
            next_page = response.meta['current'] + 1
            body = re.sub(
                r'(?<=currentPage=)\d+', str(next_page), current_request_body
            ).encode()
            yield currenct_request.replace(
                body=body,
                meta={'current': next_page, 'last': last}
            )

    def parse_docs_info(self, response):
        docs = response.xpath('//table/tbody/tr')
        for seq, it in enumerate(docs, 1):
            if it.xpath("./td[@class='no_data']"):
                break
            date_filed = it.xpath('./td[2]/text()').extract_first().strip()
            disclosure_date = parse_datetime(date_filed)
            if disclosure_date < response.meta['latest_date']:
                break
            doc_href = it.xpath('./td[5]/a/@href').extract_first()
            title = it.xpath('./td[5]/a/text()').extract_first().strip()
            end_date = it.xpath('./td[6]/text()').extract_first().strip()
            period = it.xpath('./td[7]/text()').extract_first().strip()
            notes = it.xpath('./td[8]/img/@title').extract_first()
            yield response.follow(
                doc_href,
                callback=self.parse_doc_loc,
                errback=self.errback_scraping,
                meta={
                    'company': response.meta['company'],
                    'date_filed': date_filed,
                    'title': title,
                    'end_date': end_date,
                    'period': period,
                    'notes': notes
                }
            )
            self.total_new += 1
        else:
            if 'first' in response.meta:
                last = int(response.xpath(
                    "//*[@href='#last']/@onclick").re_first(r'(?<=\()\d+'))
            else:
                last = response.meta['last']

            if response.meta['current'] < last:
                currenct_request = response.request
                current_request_body = currenct_request.body.decode()
                next_page = response.meta['current'] + 1
                body = re.sub(
                    r'(?<=currentPage=)\d+', str(next_page),
                    current_request_body
                ).encode()
                yield currenct_request.replace(
                    body=body,
                    meta={
                        'current': next_page, 'last': last,
                        'company': response.meta['company'],
                        'latest_date': response.meta['latest_date']
                    }
                )

    def parse_doc_loc(self, response):
        """不直接构造附件地址，避免存在多种文件格式"""
        doc_parts = response.xpath(
            "//*[@href='#download']/@onclick").re(r"'(.*?)'")
        if doc_parts:
            yield scrapy.Request(
                self.doc_download.format(*doc_parts),
                callback=self.parse_doc_link,
                errback=self.errback_scraping,
                meta={
                    'company': response.meta['company'],
                    'date_filed': response.meta['date_filed'],
                    'title': response.meta['title'],
                    'end_date': response.meta['end_date'],
                    'period': response.meta['period'],
                    'notes': response.meta['notes']
                }
            )
        else:
            self.logger.error(
                "%s has no attached file info on %s for %s",
                response.meta['company'], response.meta['date_filed'],
                response.meta['title']
            )

    def parse_doc_link(self, response):
        entries = response.xpath('//table/tr')
        for it in entries[1:]:
            filename = it.xpath('./td[1]/text()').extract_first()
            doc_url = response.urljoin(
                it.xpath('./td[2]/a/@href').extract_first())
            doc_url = re.sub(r';.*?(?=\?)', '', doc_url)
            yield ReportItem(
                company_code=response.meta['company'],
                fiscal_year=response.meta['end_date'].split('.')[0],
                financial_statement_season_type_code=response.meta['period'],
                disclosure_date=response.meta['date_filed'],
                end_date=response.meta['end_date'],
                doc_type=filename.split('.')[-1],
                doc_source_url=doc_url,
                version=response.meta['notes'],
                file_original_title=response.meta['title'],
                file_urls=[doc_url]
            )
            self.total_new += 1

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
