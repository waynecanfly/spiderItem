"""下载、更新公司财报"""

import os
import re
import csv
import random
from copy import deepcopy
from datetime import datetime

import scrapy
import pymysql
from scrapy import signals
from dateutil.parser import parse as parse_datetime
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyDataSourceItem, ReportItem
from ..utils.mate import month_periods, title_mappings
from ..settings import FILES_STORE, DOC_PATH


class UpdateCompanyReportSpider(scrapy.Spider):
    name = 'update_company_report'

    unique_ids = []

    periods = {
        1: 'Q',
        0: 'FY'
    }

    code_pat = re.compile(r'codigoCVM=(\d+)')
    referer = (
        'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/'
        'ResumoDemonstrativosFinanceiros.aspx?codigoCvm={}&idioma=en-us'
    )

    currency_pat = re.compile(r'\((.*?)\)')

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyReportSpider, cls).from_crawler(
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
                mark from company_data_source where company_id like 'bra%'\
            """)
            self.entries = cursor.fetchall()
        conn.close()

        self.total_successed = 0
        self.total_failed = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d successed, %d failed.',
            spider.name, self.total_successed, self.total_failed
        )

    def start_requests(self):
        for it in self.entries:
            cvm_code = re.search(self.code_pat, it['download_link']).group(1)
            yield scrapy.Request(
                it['download_link'],
                callback=self.parse,
                errback=self.errback_scraping,
                headers={'Referer': self.referer.format(cvm_code)},
                meta={
                    'dont_redirect': True,
                    'handle_httpstatus_list': [302],
                    'company': it['company_id'],
                    'mark': ord(it['mark']),  # 年报或季报标识
                    'latest_url': it['latest_url'],
                    'latest_date': it['latest_date']
                }
            )

    def parse(self, response):
        company = response.meta['company']
        mark = response.meta['mark']

        if response.status == 302:  # 请求失败，保留历史状态
            return

        entries = response.css('.list-avatar-row a')
        for index, it in enumerate(entries):
            end_date, _, _, disclosure_date, _ = it.xpath(
                '@onmouseover').re(r"'(.*?)'")
            end_date = parse_datetime(end_date)
            disclosure_date = parse_datetime(disclosure_date)
            doc_url = it.xpath('@href').re(r"'(.*?)'")[0]
            if (end_date >= response.meta['latest_date'] and
                    doc_url != response.meta['latest_url']):
                if index == 0:  # 记录本次最新下载点
                    yield CompanyDataSourceItem(
                        company_id=company, latest_url=doc_url,
                        latest_date=end_date, mark=mark,
                        gmt_update=datetime.now()
                    )
                if 'rad.cvm' in doc_url:
                    title = it.xpath('text()').extract_first()
                    try:
                        version = re.search(r"(\d|\.)+$", title).group()
                        version = '0' + version[0]
                    except AttributeError:
                        version = None
                    if mark == 1:  # 季报
                        period = month_periods[end_date.month]
                    else:
                        period = 'FY'

                    unique_id = self.gen_id()
                    if unique_id in self.unique_ids:
                        unique_id = self.gen_id()
                    else:
                        self.unique_ids.append(unique_id)

                    item = ReportItem(
                        report_id=company + unique_id,
                        company_code=company, fiscal_year=end_date.year,
                        financial_statement_season_type_code=period,
                        disclosure_date=disclosure_date, end_date=end_date,
                        doc_source_url=doc_url, version=version,
                    )
                    yield scrapy.Request(
                        doc_url,
                        callback=self.request_reports,
                        errback=self.errback_scraping,
                        meta={'item': item}  # 防止页面走丢
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
        for it in entries:
            title = it.xpath('text()').extract_first()
            if it.xpath('@selected') and title in title_mappings:  # 默认报表
                report_type = title_mappings[title]
                doc_url = re.search(
                    r"location='(.+?)'", response.text).group(1)  # 获取文档链接
                item = deepcopy(response.meta['item'])

                unique_id = self.gen_id()
                if unique_id in self.unique_ids:
                    unique_id = self.gen_id()
                else:
                    self.unique_ids.append(unique_id)

                item['report_id'] = item['company_code'] + unique_id
                item['financial_statement_type_code'] = report_type
                yield response.follow(
                    doc_url,
                    callback=self.parse_report,
                    errback=self.errback_scraping,
                    meta={'item': item}
                )
            elif title in title_mappings:
                report_type = title_mappings[title]
                formdata['cmbQuadro'] = it.xpath('@value').extract_first()
                item = deepcopy(response.meta['item'])

                unique_id = self.gen_id()
                if unique_id in self.unique_ids:
                    unique_id = self.gen_id()
                else:
                    self.unique_ids.append(unique_id)
                item['report_id'] = item['company_code'] + unique_id

                item['financial_statement_type_code'] = report_type
                yield scrapy.FormRequest(
                    dst_url,
                    formdata=formdata,
                    callback=self.do_report_request,
                    errback=self.errback_scraping,
                    meta={'item': item}
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
            self.total_successed += 1

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
            item = request.meta['item']
            item['is_downloaded'] = False
            self.total_failed += 1
            yield item
