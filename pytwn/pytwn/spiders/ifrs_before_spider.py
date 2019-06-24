"""下载台湾中文财报数据（采用IFRS之前）"""

import re

import scrapy
import pymysql
from scrapy import signals, FormRequest
from dateutil.parser import parse as parse_datestr
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import BulletinItem, ReportItem


class IFRSBeforeSpider(scrapy.Spider):
    name = 'ifrs_before'

    start_urls = ['http://mops.twse.com.tw/mops/web/t05st21']
    target = '採IFRSs前'
    reports_title = ['個別報表', '合併報表', '簡明報表', '合併關係企業財務報表']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(IFRSBeforeSpider, cls).from_crawler(
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
                select code, security_code symbol, exchange_market_code \
                exchange from company where country_code_listed='TWN' and \
                (ipo_date is null or year(ipo_date) < 2013)\
            """)
            self.companies = cursor.fetchall()
        conn.close()

        self.total_new = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new', spider.name, self.total_new
        )

    def parse(self, response):
        entries = response.xpath(
            "//a[text()='{}']/following-sibling::span[1]/span".format(
                self.target
            )
        )
        for each in entries:
            title = each.xpath('./a/text()').extract_first()
            if title in self.reports_title:
                is_report = True
            else:
                is_report = False
            if not each.xpath('./span'):  # 无子标题
                href = each.xpath('./a/@href').extract_first()
                if not href:
                    self.logger.error('%s has no href', title)
                    continue
                yield response.follow(
                    href,
                    callback=self.search_docs,
                    errback=self.errback_scraping,
                    meta={
                        'title': '{}_{}'.format(title, self.target),
                        'is_report': is_report,
                        'count': 1
                    }
                )
            else:
                for every in each.xpath('./span//a'):
                    subtitle = every.xpath('string()').extract_first()
                    href = every.xpath('@href').extract_first()
                    if not href:
                        self.logger.error('%s %s has no href', title, subtitle)
                        continue
                    yield response.follow(
                        href,
                        callback=self.search_docs,
                        errback=self.errback_scraping,
                        meta={
                            'title': '{}_{}_{}'.format(
                                subtitle, title, self.target
                            ),
                            'is_report': is_report,
                            'count': 1
                        }
                    )

    def search_docs(self, response):
        if '財務報告更(補)正查詢' in response.meta['title']:
            markets = response.xpath(
                "//select[@name='TYPEK']/option/@value").extract()
            seasons = response.xpath(
                "//select[@name='season']/option[@value!='']/@value").extract()
            for market in markets:
                for year in range(96, 102):
                    for season in seasons:
                        yield FormRequest.from_response(
                            response,
                            formid='form1',
                            formdata={
                                'encodeURIComponent': '1',
                                'firstin': '1',
                                'TYPEK': market,
                                'year': str(year),
                                'season': season
                            },
                            callback=self.check_query_jobs,
                            errback=self.errback_scraping,
                            meta={
                                'year': str(year),
                                'season': season,
                                'title': response.meta['title'],
                                'is_report': response.meta['is_report'],
                                'count': 1
                            }
                        )

        for it in self.companies:  # 最新资料
            yield FormRequest.from_response(
                response,
                formid='form1',
                formdata={
                    'co_id': it['symbol'], 'encodeURIComponent': '1'
                },
                callback=self.check_doc,
                errback=self.errback_scraping,
                meta={
                    'cid': it['symbol'],
                    'code': it['code'],
                    'exchange': it['exchange'],
                    'year': None,
                    'season': None,
                    'title': response.meta['title'],
                    'is_report': response.meta['is_report'],
                    'count': 1
                }
            )

        seasons = response.xpath(
            "//select[@name='season']/option[@value!='']/@value").extract()
        for it in self.companies:  # 历史资料
            for year in range(96, 102):  # 07年至12年
                if seasons:
                    for season in seasons:
                        yield FormRequest.from_response(
                            response,
                            formid='form1',
                            formdata={
                                'isnew': 'false',
                                'year': str(year),
                                'season': season,
                                'co_id': it['symbol'],
                            },
                            callback=self.check_doc,
                            errback=self.errback_scraping,
                            meta={
                                'cid': it['symbol'],
                                'code': it['code'],
                                'exchange': it['exchange'],
                                'year': str(year),
                                'season': season,
                                'title': response.meta['title'],
                                'is_report': response.meta['is_report'],
                                'count': 1
                            }
                        )
                else:
                    yield FormRequest.from_response(
                        response,
                        formid='form1',
                        formdata={
                            'year': str(year),
                            'co_id': it['symbol'],
                            'encodeURIComponent': '1'
                        },
                        callback=self.check_doc,
                        errback=self.errback_scraping,
                        meta={
                            'cid': it['symbol'],
                            'code': it['code'],
                            'exchange': it['exchange'],
                            'year': str(year),
                            'season': None,
                            'title': response.meta['title'],
                            'is_report': response.meta['is_report'],
                            'count': 1
                        }
                    )
        # '''

    def check_query_jobs(self, response):
        """处理'財務報告更(補)正查詢作業'详细资料"""

        entries = response.xpath(
            "//table[@class='hasBorder']/tr[position()>1]")
        for each in entries:
            co_id = each.xpath('./td[1]/text()').extract_first()
            for every in self.companies:
                if co_id == every['symbol']:
                    date_ = each.xpath('./td[3]/text()').extract_first()
                    caption = each.xpath('./td[5]/text()').extract_first()
                    detail = each.xpath(
                        './td[6]/input/@onclick').extract_first()
                    skey, cid, rid, dtype, _ = re.findall(r'"(.*?)"', detail)
                    yield FormRequest.from_response(
                        response,
                        formname='fm',
                        formdata={
                            'SKEY': skey, 'CID': cid,
                            'RID': rid, 'DTYPE': dtype
                        },
                        callback=self.parse_doc_detail,
                        errback=self.errback_scraping,
                        meta={
                            'cid': cid,
                            'code': every['code'],
                            'exchange': every['exchange'],
                            'year': response.meta['year'],
                            'season': response.meta['season'],
                            'date': date_,
                            'caption': caption,
                            'title': response.meta['title'],
                            'is_report': response.meta['is_report'],
                            'count': 1
                        }
                    )
                    break

    def parse_doc_detail(self, response):
        """处理'財務報告更(補)正查詢作業'公告内容，附件"""
        attaches = response.xpath(
            "//th[contains(text(), '附件')]/following-sibling::td/a/@href"
        ).extract()
        file_urls = [
            response.urljoin(it).replace('\n', '') for it in attaches
        ]
        if response.meta['is_report']:  # 暂定位非财报
            pass
        else:
            detail_type = '{}_Q{}'.format(
                response.meta['title'], response.meta['season'][1]
            )
            self.total_new += 1
            yield BulletinItem(
                exchange_market_code=response.meta['exchange'],
                company_code=response.meta['code'],
                fiscal_year=response.meta['year'],
                disclosure_date=parse_datestr(response.meta['date']),
                doc_type='directory',
                file_original_title=response.meta['caption'],
                announcement_detail_type=detail_type,
                is_downloaded=response.meta['is_downloaded'],
                data=response.body,
                file_urls=file_urls
            )

    def check_doc(self, response):
        """检查是否有对应的公告或财报"""
        if response.xpath("//form[@name='autoForm']"):  # 自动提交一步表单
            yield FormRequest.from_response(
                response,
                formname='autoForm',
                formdata={'run': 'Y'},
                callback=self.check_doc,
                errback=self.errback_scraping,
                meta={
                    'cid': response.meta['cid'],
                    'exchange': response.meta['exchange'],
                    'code': response.meta['code'],
                    'year': response.meta['year'],
                    'season': response.meta['season'],
                    'title': response.meta['title'],
                    'is_report': response.meta['is_report'],
                    'count': 1
                }
            )
        else:
            if (response.xpath("//font[text()='查無資料']") or
                    response.xpath("//h4[contains(string(), '資料庫中查無需求資料')]")):
                pass
            else:
                if response.meta['is_report']:
                    if response.meta['season']:
                        title = '{}_Q{}'.format(
                            response.meta['title'],
                            response.meta['season'][1],
                        )
                    else:
                        title = response.meta['title']  # 最新资料无季度
                    self.total_new += 1
                    yield ReportItem(
                        exchange_market_code=response.meta['exchange'],
                        company_code=response.meta['code'],
                        fiscal_year=response.meta['year'],
                        doc_source_url=response.meta['doc_url'],
                        announcement_detail_type=title,
                        is_downloaded=response.meta['is_downloaded'],
                        data=response.body
                    )
                else:
                    self.total_new += 1
                    yield BulletinItem(
                        exchange_market_code=response.meta['exchange'],
                        company_code=response.meta['code'],
                        fiscal_year=response.meta['year'],
                        doc_source_url=response.meta['doc_url'],
                        announcement_detail_type=response.meta['title'],
                        is_downloaded=response.meta['is_downloaded'],
                        data=response.body
                    )

    def errback_scraping(self, failure):
        request = failure.request
        req_url = request.url
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
