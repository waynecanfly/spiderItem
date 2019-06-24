"""下载台湾中文财报数据（采用IFRS之后）"""

from datetime import date

import scrapy
import pymysql
from scrapy import signals, FormRequest
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import ReportItem


class IFRSAfterSpider(scrapy.Spider):
    name = 'ifrs_after'

    start_urls = ['http://mops.twse.com.tw/server-java/t164sb01']
    target = '採IFRSs後'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(IFRSAfterSpider, cls).from_crawler(
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
                select t1.code, t1.security_code symbol, \
                t1.exchange_market_code exchange, t2.last_year from company \
                t1 left join (select company_code, max(fiscal_year) last_year \
                from financial_statement_index where country_code='twn' and \
                user_create='lq' group by company_code) t2 on \
                t1.code=t2.company_code where t1.country_code_listed='twn'\
            """)
            self.entries = cursor.fetchall()
        conn.close()

        self.total_new = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new', spider.name, self.total_new
        )

    def parse(self, response):
        seasons = response.xpath(
            "//select[@name='SSEASON']/option/@value").extract()
        report_ids = response.xpath(
            "//select[@name='REPORT_ID']/option/@value").extract()
        report_types = response.xpath(
            "//select[@name='REPORT_ID']/option/text()").extract()
        report_types = [it.strip() for it in report_types]

        end_year = date.today().year + 1
        for season in seasons:  # 季度遍历
            for index, id_ in enumerate(report_ids):  # 报表类型遍历
                title = '{}_{}_Q{}'.format(
                    report_types[index], self.target, season
                )  # 基于季度和报表类型确定报表分类
                for each in self.entries:  # 遍历公司
                    for year in range(2013, end_year):
                        if each['last_year'] and each['last_year'] > year:
                            break  # 该年份财报数据已下载，跳过
                        yield FormRequest.from_response(
                            response,
                            formdata={
                                'CO_ID': each['symbol'],
                                'SYEAR': str(year),
                                'SSEASON': season,
                                'REPORT_ID': id_,
                                'qryId': None,
                                'query': None,
                            },
                            callback=self.parse_doc,
                            errback=self.errback_scraping,
                            meta={
                                'cid': each['symbol'],
                                'code': each['code'],
                                'exchange': each['exchange'],
                                'year': year,
                                'season': season,
                                'title': title,
                            }
                        )

    def parse_doc(self, response):
        if response.xpath("//font[text()='查無資料']"):
            return

        title = response.xpath('//span/text()').extract_first()
        self.total_new += 1
        yield ReportItem(
            exchange_market_code=response.meta['exchange'],
            company_code=response.meta['code'],
            fiscal_year=response.meta['year'],
            doc_source_url=response.meta['doc_url'],
            file_original_title=title,
            announcement_detail_type=response.meta['title'],
            is_downloaded=response.meta['is_downloaded'],
            data=response.body
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
