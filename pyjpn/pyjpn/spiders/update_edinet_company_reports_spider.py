"""Update reports in edinet"""

from urllib.parse import urlencode

import scrapy
import pymysql
from scrapy import signals
from dateutil.parser import parse as parse_datetime
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import ReportItem, CompanyDataSourceItem


class UpdateEdinetCompanyReportsSpider(scrapy.Spider):
    name = 'update_edinet_company_reports'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateEdinetCompanyReportsSpider, cls).from_crawler(
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
                select company_id, download_link, latest_url, latest_date, \
                info_disclosure_id from company_data_source where company_id \
                like 'JPN%' and user_create='lq'\
            """)
            self.entries = cursor.fetchall()
        conn.close()

        self.total_docs = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d reports.', spider.name,
            self.total_docs
        )

    def start_requests(self):
        for it in self.entries:
            if it['latest_url']:
                latest_word = it['latest_url'].split('&')[-1]
            else:
                latest_word = str(it['latest_url'])
            yield scrapy.Request(
                it['download_link'],
                callback=self.parse,
                errback=self.errback_scraping,
                meta={
                    'company': it['company_id'],
                    'latest_date': it['latest_date'],
                    'latest_word': latest_word,
                    'edinet_code': it['info_disclosure_id']
                }
            )

    def parse(self, response):
        entries = response.xpath(
            "//table[contains(@class, 'result')]/tr[position()>1]")
        if not entries:  # Can't find docs info for this company
            return

        PID = response.xpath("//input[@name='PID']/@value").extract_first()
        req_args = {
            'uji.verb': '',
            'uji.bean': '',
            'PID': PID
        }
        for index, it in enumerate(entries, 1):
            disclosure_date = it.xpath(
                './td[1]/div/text()').extract_first().strip()
            disclosure_date = parse_datetime(disclosure_date)
            # no confirmation letter
            title = it.xpath('./td[2]/a/text()').extract_first('').strip()
            if disclosure_date < response.meta['latest_date']:
                break

            pdf_href = it.xpath('./td[6]//a/@href').extract_first()
            if (title and ('Quarterly' in title or 'Annual' in title) and
                    not pdf_href.endswith(response.meta['latest_word'])):
                self.total_docs += 1
                pdf_url = response.urljoin(pdf_href) if pdf_href else pdf_href

                if index == 1:  # 记录最新下载点
                    yield CompanyDataSourceItem(
                        company_id=response.meta['company'],
                        latest_url=pdf_url, latest_date=disclosure_date
                    )

                xbrl_parts = it.xpath('./td[7]//a/@onclick').re(r"\((.*?)\)")
                if xbrl_parts:
                    verb, bean, pid, tid, other, url = xbrl_parts[0].replace(
                        "'", '').replace(' ', '').split(',')
                    req_args['uji.verb'] = verb
                    req_args['uji.bean'] = bean
                    download_url = response.urljoin(url)
                    if '?' in download_url:
                        download_url = download_url.split('?')[0] + '?'
                    xbrl_url = download_url + urlencode(req_args) + '&' + other
                    yield ReportItem(
                        company_code=response.meta['company'],
                        disclosure_date=disclosure_date, doc_type='zip',
                        doc_source_url=xbrl_url, file_original_title=title,
                        file_urls=[xbrl_url]
                    )
                else:
                    yield ReportItem(
                        company_code=response.meta['company'],
                        disclosure_date=disclosure_date, doc_type='pdf',
                        doc_source_url=pdf_url, file_original_title=title,
                        file_urls=[pdf_url]
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
