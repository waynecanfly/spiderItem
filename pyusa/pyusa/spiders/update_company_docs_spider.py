"""下载、更新公司财报"""

import re
from datetime import datetime

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyDataSourceItem, AnnounceItem, ReportItem
from ..utils.mate import periods_mapping


class UpdateCompanyDocsSpider(scrapy.Spider):
    name = 'update_company_docs'

    expected_terms = [
        '10-Q', '10-K', '10-Q/A', '10-K/A', '6-K', '6-K/A',
        '20-F', '20-F/A', '40-F', '40-F/A'
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyDocsSpider, cls).from_crawler(
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
                select ta.code, ta.exchange_market_code, tb.download_link, \
                tb.latest_url, tb.latest_date from company ta left join \
                company_data_source tb on ta.code = tb.company_id where \
                country_code_listed='USA'\
                """)
            self.entries = cursor.fetchall()

        self.total_companies = 0
        self.total_docs = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s, %s companies with %s docs...', spider.name,
            self.total_companies, self.total_docs
        )

    def start_requests(self):
        for it in self.entries[4000:5000]:
            if it['download_link']:
                yield scrapy.Request(
                    it['download_link'],
                    callback=self.parse,
                    errback=self.errback_scraping,
                    meta={
                        'company': it['code'],
                        'exchange': it['exchange_market_code'],
                        'latest_url': it['latest_url'],
                        'latest_date': str(it['latest_date']),
                        'first': True  # 第一页标识项
                    }
                )

    def parse(self, response):
        response.selector.register_namespace(
            'ns', 'http://www.w3.org/2005/Atom'
        )

        company = response.meta['company']
        exchange = response.meta['exchange']
        latest_url = response.meta['latest_url']
        latest_date = response.meta['latest_date']
        entries = response.xpath('//ns:entry')
        if 'first' in response.meta and entries:  # 记录最新下载点
            current_first_url = entries[0].xpath(
                './ns:content/ns:filing-href/text()').extract_first()
            current_first_date = entries[0].xpath(
                './ns:content/ns:filing-date/text()').extract_first()
            if (latest_url != current_first_url and
                    current_first_date >= latest_date):
                self.total_companies += 1
                yield CompanyDataSourceItem(
                    company_id=company, latest_url=current_first_url,
                    latest_date=current_first_date, gmt_update=datetime.now(),
                    user_update='lq'
                )

        if entries:  # 当页有内容才进行动作，避免陷入无限循环
            for it in entries:
                filing_date = it.xpath(
                    './ns:content/ns:filing-date/text()').extract_first()
                filing_url = it.xpath(
                    './ns:content/ns:filing-href/text()').extract_first()
                if filing_date >= latest_date and filing_url != latest_url:
                    self.total_docs += 1
                    acc_no = it.xpath(
                        './ns:content/ns:accession-nunber/text()'
                    ).extract_first()
                    file_number = it.xpath(
                        './ns:content/ns:file-number/text()').extract_first()
                    filing_type = it.xpath(
                        './ns:content/ns:filing-type/text()').extract_first()
                    film_number = it.xpath(
                        './ns:content/ns:film-number/text()').extract_first()
                    xbrl_href = it.xpath(
                        './ns:content/ns:xbrl_href/text()').extract_first()
                    title = it.xpath('./ns:title/text()').extract_first()
                    ff_number = str(file_number) + '_' + str(film_number)
                    yield scrapy.Request(
                        filing_url,
                        callback=self.parse_filing,
                        errback=self.errback_scraping,
                        meta={
                            'company': company,
                            'exchange': exchange,
                            'filing_date': filing_date,
                            'filing_url': filing_url,
                            'acc_no': acc_no,
                            'xbrl_href': xbrl_href,
                            'title': title,
                            'filing_type': filing_type,
                            'ff_number': ff_number
                        }
                    )
                else:
                    break
            else:  # 当页没有找到上次下载点，继续下一页
                next_page = re.sub(
                    r'(?<=start=)(\d+)',
                    lambda m: str(int(m.group(1)) + 100),
                    response.url
                )
                yield scrapy.Request(
                    next_page,
                    callback=self.parse,
                    errback=self.errback_scraping,
                    meta={
                        'company': company, 'exchange': exchange,
                        'latest_url': latest_url, 'latest_date': latest_date
                    }
                )

    def parse_filing(self, response):
        end_date = response.xpath(
            "//div[text()='Period of Report']/following-sibling::div[1]/text()"
        ).extract_first()
        if end_date and re.match(r'\d{4}\-', end_date):
            fiscal_year = end_date.split('-')[0]
        else:
            fiscal_year = response.meta['filing_date'].split('-')[0]

        file_urls = response.xpath(
            "//table[@summary='Document Format Files']/tr/td[3]/a/@href"
        ).extract()
        file_urls = set(
            response.urljoin(x) for x in file_urls if not x.endswith('/')
        )
        xbrl_urls = set()

        is_direct_item = False  # 是否直接按公告下载
        data_table = response.xpath("//table[@summary='Data Files']")
        if data_table:  # xbrl文件，尝试解析xbrl信息
            xbrl_urls = data_table.xpath('./tr/td[3]/a/@href').extract()
            xbrl_urls = set(
                response.urljoin(x) for x in xbrl_urls if not x.endswith('/')
            )
            if response.meta['filing_type'] in self.expected_terms:  # 财报类型
                if response.meta['xbrl_href']:
                    yield scrapy.Request(
                        response.meta['xbrl_href'],
                        callback=self.parse_xbrl_r1_link,
                        errback=self.errback_scraping,
                        meta={
                            'company': response.meta['company'],
                            'exchange': response.meta['exchange'],
                            'filing_type': response.meta['filing_type'],
                            'disclosure_date': response.meta['filing_date'],
                            'end_date': end_date,
                            'filing_url': response.meta['filing_url'],
                            'title': response.meta['title'],
                            'filing_type': response.meta['filing_type'],
                            'acc_no': response.meta['acc_no'],
                            'file_film_number': response.meta['ff_number'],
                            'file_urls': xbrl_urls,
                        }
                    )
                else:  # 以公告形式下载
                    is_direct_item = True
            else:  # 非预期类型，以公告形式下载
                is_direct_item = True
        else:  # 公告文件
            is_direct_item = True

        file_urls = file_urls | xbrl_urls
        if is_direct_item and file_urls:
            yield AnnounceItem(
                company_code=response.meta['company'],
                exchange_market_code=response.meta['exchange'],
                fiscal_year=fiscal_year,
                disclosure_date=response.meta['filing_date'],
                doc_source_url=response.meta['filing_url'],
                is_doc_url_direct=False,
                file_original_title=response.meta['title'],
                announcement_detail_type=response.meta['filing_type'],
                acc_no=response.meta['acc_no'],
                file_film_number=response.meta['ff_number'],
                file_urls=file_urls
            )

    def parse_xbrl_r1_link(self, response):
        r1 = re.search(
            r'reports\[0\+1\]\s?=\s?"(.*?)"', response.text
        )
        if r1:
            yield response.follow(
                r1.group(1),
                callback=self.parse_xbrl_r1_info,
                errback=self.errback_scraping,
                meta={
                    'company': response.meta['company'],
                    'exchange': response.meta['exchange'],
                    'filing_type': response.meta['filing_type'],
                    'disclosure_date': response.meta['disclosure_date'],
                    'end_date': response.meta['end_date'],
                    'filing_url': response.meta['filing_url'],
                    'title': response.meta['title'],
                    'filing_type': response.meta['filing_type'],
                    'acc_no': response.meta['acc_no'],
                    'file_film_number': response.meta['file_film_number'],
                    'file_urls': response.meta['file_urls']
                }
            )
        else:
            self.logger.warning('%s has no r1 page', response.url)

    def parse_xbrl_r1_info(self, response):
        period_labels = [
            'document fiscal period focus', 'document period focus',
            'document fiscal period focus (q1,q2,q3,fy)'
        ]
        year_labels = [
            'document fiscal year focus', 'document fiscal year end focus'
        ]
        expected_periods = ['Q1', 'Q2', 'Q3', 'Q4', 'FY', 'H1']
        fiscal_period, fiscal_year = None, None

        if response.url.endswith('htm'):
            entries = response.xpath("//*[@class='report']/tr")
            for it in entries:
                label = it.xpath('string(./td[1])').extract_first(
                    default='').lower()
                if label in period_labels:
                    values = [
                        x.xpath('string()').extract_first(default='').strip()
                        for x in it.xpath('./td[position()>1]')
                    ]
                    for value in values:
                        if value in expected_periods:
                            fiscal_period = value
                            break
                elif label in year_labels:
                    values = [
                        x.xpath('string()').extract_first(default='').strip()
                        for x in it.xpath('./td[position()>1]')
                    ]
                    for value in values:
                        if re.match(r'\d{4}$', value):
                            fiscal_year = value
                            break
                if fiscal_period and fiscal_year:
                    break
            else:
                self.logger.warning(
                    '%s has no expected period and year htm.',
                    response.url
                )
        elif response.url.endswith('xml'):
            self.logger.warning('%s has r1 xml', response.url)
        else:
            self.logger.warning('%s is not htm or xml', response.url)

        if fiscal_period and fiscal_year:
            if fiscal_period in periods_mapping:
                fiscal_period = periods_mapping[fiscal_period]
            else:
                fiscal_period = '00'
            yield ReportItem(
                company_code=response.meta['company'], fiscal_year=fiscal_year,
                financial_statement_season_type_code=fiscal_period,
                disclosure_date=response.meta['disclosure_date'],
                end_date=response.meta['end_date'],
                doc_source_url=response.meta['filing_url'],
                file_original_title=response.meta['title'],
                version=response.meta['filing_type'],
                acc_no=response.meta['acc_no'],
                file_film_number=response.meta['file_film_number'],
                file_urls=response.meta['file_urls']
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
