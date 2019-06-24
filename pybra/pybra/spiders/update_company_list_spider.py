"""更新上市公司列表"""

from datetime import datetime

import scrapy
import pymysql
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import CompanyItem, CompanyDataSourceItem

class UpdateCompanyListSpider(scrapy.Spider):
    name = 'update_company_list'

    ajax_url = (
        'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/'
        'BuscaEmpresaListada.aspx?idioma=en-us'
    )

    quarterly_url = (
        'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/'
        'HistoricoFormularioReferencia.aspx?codigoCVM={}&tipo=itr&ano=0'
        '&idioma=en-us'
    )
    annual_url = (
        'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/'
        'HistoricoFormularioReferencia.aspx?codigoCVM={}&tipo=dfp&ano=0'
        '&idioma=en-us'
    )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyListSpider, cls).from_crawler(
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
                select code, name_origin, remarks from company where \
                country_code_listed='BRA'\
                """)
            records = cursor.fetchall()
        conn.close()


        self.remarks = [it['remarks'] for it in records]
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

    def start_requests(self):
        yield scrapy.FormRequest(
            self.ajax_url,
            method='POST',
            formdata={
                '__EVENTTARGET': 'ctl00:contentPlaceHolderConteudo:BuscaNomeEmpresa1:btnTodas',
                'RadAJAXControlID': 'ctl00_contentPlaceHolderConteudo_AjaxPanelBusca',
                'httprequest': '1'
            },
            callback=self.parse_companies,
            errback=self.errback_scraping,
        )

    def parse_companies(self, response):
        entries = response.xpath('//table/tbody/tr')
        for it in entries:
            company = it.xpath('./td[1]/a/text()').extract_first()
            link = response.urljoin(
                it.xpath('./td[1]/a/@href').extract_first()
            )
            cvm_code = link.split('=')[-1]
            quotation = it.xpath('./td[3]/text()').extract_first().strip()
            quotation = quotation if quotation else None
            if link not in self.remarks:
                self.total_new += 1
                self.max_code_num += 1
                company_code = 'BRA' + str(self.max_code_num)
                yield CompanyItem(
                    code=company_code, name_origin=company,
                    remarks=link, gmt_create=datetime.now()
                )
                # 季报链接
                yield CompanyDataSourceItem(
                    company_id=company_code, company_name=company,
                    download_link=self.quarterly_url.format(cvm_code),
                    is_batch=True, mark=True, gmt_create=datetime.now()
                )
                # 年报链接
                yield CompanyDataSourceItem(
                    company_id=company_code, company_name=company,
                    download_link=self.annual_url.format(cvm_code),
                    is_batch=True, mark=False, gmt_create=datetime.now()
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
