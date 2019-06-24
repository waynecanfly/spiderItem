"""处理下载失败的情况"""

import os
import re
import csv
import random
from copy import deepcopy
from datetime import datetime

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import ReportItem
from ..utils.mate import title_mappings, report_type_titles
from ..settings import FILES_STORE, DOC_PATH


class UpdateCompanyReportMateSpiderMate(scrapy.Spider):
    name = 'update_company_report_mate'

    currency_pat = re.compile(r'\((.*?)\)')

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyReportMateSpiderMate, cls).from_crawler(
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
                select report_id, company_code, fiscal_year, disclosure_date, \
                end_date, financial_statement_type_code, doc_source_url,\
                financial_statement_season_type_code, version from \
                financial_statement_index where country_code='BRA' and \
                is_downloaded=0\
            """)
            self.entries = cursor.fetchall()
        conn.close()

        self.total = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d reports.', spider.name, self.total)

    def start_requests(self):
        for it in self.entries:  # 遍历未下载成功的报表
            item = ReportItem(it)
            yield scrapy.Request(
                it['doc_source_url'],
                callback=self.request_reports,
                errback=self.errback_scraping,
                meta={'item': item}
            )

    def request_reports(self, response):
        grupo = response.xpath(
            "//*[@name='cmbGrupo']/option[@selected]/@value").extract_first()
        formdata = {
            '__EVENTTARGET': 'cmbQuadro',
            'cmbGrupo': grupo,
            'cmbQuadro': None
        }
        hidden_names = response.xpath('//input/@name').extract()
        hidden_vals = response.xpath('//input/@value').extract()
        formdata.update(dict(zip(hidden_names, hidden_vals)))
        dst_url = response.urljoin(
            response.xpath('//form/@action').extract_first())

        entries = response.xpath("//*[@name='cmbQuadro']/option")

        item = response.meta['item']
        if item['financial_statement_type_code']:  # 缺某种类型的报表
            title = report_type_titles[item['financial_statement_type_code']]
            for it in entries:
                if title == it.xpath('text()').extract_first():
                    if it.xpath('@selected'):
                        doc_url = re.search(
                            r"location='(.+?)'", response.text).group(1)
                        yield response.follow(
                            doc_url,
                            callback=self.parse_report,
                            errback=self.errback_scraping,
                            meta={'item': item}
                        )
                    else:
                        formdata['cmbQuadro'] = it.xpath(
                            '@value').extract_first()
                        yield scrapy.FormRequest(
                            dst_url,
                            formdata=formdata,
                            callable=self.do_report_request,
                            errback=self.errback_scraping,
                            meta={'item': item}
                        )
                    break
        else:  # 缺该报表所有类型的数据
            for it in entries:
                title = it.xpath('text()').extract_first()
                if it.xpath('@selected') and title in title_mappings:  # 默认报表
                    report_type = title_mappings[title]
                    doc_url = re.search(
                        r"location='(.+?)'", response.text).group(1)
                    item_ = deepcopy(item)
                    item_['parent_id'] = item['report_id']
                    item_['report_id'] = item_['company_code'] + self.gen_id()
                    item_['financial_statement_type_code'] = report_type
                    yield response.follow(
                        doc_url,
                        callback=self.parse_report,
                        errback=self.errback_scraping,
                        meta={'item': item_}
                    )
                elif title in title_mappings:
                    report_type = title_mappings[title]
                    formdata['cmbQuadro'] = it.xpath('@value').extract_first()
                    item_ = deepcopy(response.meta['item'])
                    item_['parent_id'] = item['report_id']
                    item_['report_id'] = item_['company_code'] + self.gen_id()
                    item_['financial_statement_type_code'] = report_type
                    yield scrapy.FormRequest(
                        dst_url,
                        formdata=formdata,
                        callback=self.do_report_request,
                        errback=self.errback_scraping,
                        meta={'item': item_}
                    )

    def do_report_request(self, response):
        doc_url = re.search(r"location='(.+?)'", response.text).group(1)
        yield response.follow(
            doc_url,
            callback=self.parse_report,
            errback=self.errback_scraping,
            meta={'item': response.meta['item']}
        )

    def parse_report(self, response):
        item = response.meta['item']

        entries = response.xpath('//table/tr')
        if entries:
            title = response.xpath(
                "//*[@id='TituloTabelaSemBorda']/text()").extract_first()
            if re.search(self.currency_pat, title):
                currency = re.search(self.currency_pat, title).group(1)
            else:
                currency = None
            item['currency_code'] = currency

            data = []
            for each in entries:
                cols = each.xpath('./td')
                row_data = []
                for it in cols:
                    val = it.xpath('string()').extract_first().strip()
                    row_data.append(val)
                data.append(row_data)

            for index, line in enumerate(data[0][2:], 2):  # 替换日期间隔符
                data[0][index] = re.sub(r'\s+a\s+', '-', data[0][index])

            file_dir = os.path.join(FILES_STORE, str(item['fiscal_year']))
            if not os.path.exists(file_dir):
                os.mkdir(file_dir)

            filename = item['report_id'] + '.csv'
            csv_file = os.path.join(file_dir, filename)
            with open(csv_file, 'w', encoding='utf-8', newline='') as f:
                f_csv = csv.writer(f)
                f_csv.writerows(data)

            item['doc_local_path'] = DOC_PATH.format(
                item['fiscal_year'], filename)
            item['doc_downloaded_timestamp'] = datetime.now()
            item['is_downloaded'] = True
            item['file_original_title'] = title

            yield item
        else:  # 请求报表数据失败，在数据库中标记为未下载状态
            item['is_downloaded'] = False
            yield item

    def gen_id(self):
        time_id = str(datetime.now()).split(".")[-1]
        random_id1 = str(random.randrange(0, 9))
        random_id2 = str(random.randrange(0, 9))
        unique_id = time_id + random_id1 + random_id2
        return unique_id

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

        if 'item' in request.meta:  # 未正确处理的报表
            yield request.meta['item']
