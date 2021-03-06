"""下载、更新公司文件"""

import re
from urllib.parse import urlencode
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

from ..items import AnnounceItem, CompanyDataSourceItem


class UpdateCompanyDocsSpider(scrapy.Spider):
    name = 'update_company_docs_bak'

    start_urls = ['https://www.moex.com/ru/listing/emidocs.aspx']

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
                select code, name_origin, security_code from company where \
                country_code_listed='RUS' and name_origin is not null\
            """)
            self.companies = cursor.fetchall()
        conn.close()

        self.total_new = 0

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s..., %d new', spider.name, self.total_new
        )

    def parse(self, response):
        data = {'type': 1, 'iss': None}
        for each in self.companies:
            data['iss'] = each['name_origin']
            yield scrapy.Request(
                self.start_urls[0] + '?' + urlencode(data),
                callback=self.parse_docs_index,
                errback=self.errback_scraping,
                meta={
                    'code': each['code'], 'name': each['name_origin'],
                    'symbol': each['security_code']
                }
            )

    def parse_docs_index(self, response):
        entries = response.xpath('//form//table/tr')
        if entries:
            for it in entries:
                docs_href = it.xpath('./td[2]/a/@href').extract_first()
                if docs_href:
                    yield response.follow(
                        docs_href,
                        callback=self.parse_docs,
                        errback=self.errback_scraping,
                        meta={
                            'code': response.meta['code'],
                            'name': response.meta['name'],
                            'symbol': response.meta['symbol']
                        }
                    )
                    break

    def parse_docs(self, response):
        hiddens = response.xpath("//input[@type='hidden' and boolean(@id)]")
        ids = hiddens.xpath('@id').extract()

        dates = []
        for index, each in enumerate(hiddens):
            id_ = ids[index]
            caption = each.xpath(
                "following-sibling::h3/text()").extract_first()
            entries = each.xpath("following-sibling::table[1]/tr[boolean(td)]")
            for index, it in enumerate(entries):
                items = self.parse_doc_info(response, it, id_, caption)
                for item in items:
                    self.total_new += 1
                    yield item
                if index == 1 and items:
                    dates.append(items[0]['disclosure_date'])

            # next page
            if id_ not in ['pge_facts']:
                next_req = self.do_next_page(response, each, id_, caption)
                if next_req:
                    yield next_req

        yield CompanyDataSourceItem(
            company_id=response.meta['code'],
            company_name=response.meta['name'],
            security_code=response.meta['symbol'],
            download_link=response.url,
            latest_date=min(dates),
            is_batch=True,
            gmt_create=datetime.now(),
            user_create='lq'
        )

    def parse_next_docs(self, response):
        id_ = response.meta['id']
        caption = response.meta['caption']
        entries = response.xpath((
            "//input[@id='{}']/following-sibling::table[1]/tr[boolean(td)]"
        ).format(id_))
        for it in entries:
            items = self.parse_doc_info(response, it, id_, caption)
            for item in items:
                self.total_new += 1
                yield item

        # next page
        input_e = response.xpath("//input[@id='{}']".format(id_))
        next_req = self.do_next_page(response, input_e, id_, caption)
        if next_req:
            yield next_req

    def do_next_page(self, response, input_e, id_, caption):
        next_page = input_e.xpath(
            "following-sibling::div//span[@_act]/following-sibling::a/@href"
        ).extract_first()
        if next_page and id_.split('_')[-1] in next_page.lower():
            next_page = re.search(r'(?<=\()\d+', next_page).group()
            hiddens = response.xpath("//input[@type='hidden']")
            hidden_names = hiddens.xpath('@name').extract()
            hidden_vals = hiddens.xpath('@value').extract()
            formdata = dict(zip(hidden_names, hidden_vals))
            formdata[id_] = next_page
            return scrapy.Request(
                self.start_urls[0] + '?' + urlencode(formdata),
                callback=self.parse_next_docs,
                errback=self.errback_scraping,
                meta={
                    'id': id_, 'caption': caption,
                    'code': response.meta['code']
                }
            )

    def parse_doc_info(self, response, it, id_, caption):
        docs, items = [], []
        if id_ in ['pge_rns', 'pge_edgar']:
            date_str = it.xpath('./td[1]/text()').extract_first()
            url = it.xpath('./td[2]/a/@href').extract_first()
            title = it.xpath('./td[2]/a/text()').extract_first()
            docs.append([date_str, url, title, 'en', None])
        elif id_ in ['pge_emiss']:
            url = it.xpath('./td[1]/a/@href').extract_first()
            title = it.xpath('./td[1]/a/text()').extract_first()
            date_str = it.xpath('./td[3]/text()').extract_first()
            docs.append([date_str, url, title, 'ru', None])
        elif id_ in ['pge_etf']:
            end_date = it.xpath('./td[1]/text()').extract_first()
            ru_url = it.xpath('./td[2]/a/@href').extract_first()
            ru_title = it.xpath('./td[2]/a/text()').extract_first()
            ru_date = it.xpath('./td[3]/text()').extract_first()
            ru_doc = [ru_date, ru_url, ru_title, 'ru', end_date]
            if ru_url:
                docs.append(ru_doc)
            en_url = it.xpath('./td[4]/a/@href').extract_first()
            en_title = it.xpath('./td[4]/a/text()').extract_first()
            en_date = it.xpath('./td[5]/text()').extract_first()
            en_doc = [en_date, en_url, en_title, 'en', end_date]
            if en_url:
                docs.append(en_doc)
        for doc in docs:
            if not doc[1].startswith('http'):
                doc[1] = response.urljoin(doc[1])
            if '.' in doc[1]:
                doc_type = doc[1].split('.')[-1]
            else:
                doc_type = 'unknow'
            if doc[4]:
                doc[4] = parse_datetime(doc[4])
            items.append(AnnounceItem(
                company_code=response.meta['code'],
                disclosure_date=parse_datetime(doc[0]),
                end_date=doc[4], language_written_code=doc[3],
                doc_type=doc_type, doc_source_url=doc[1],
                file_original_title=doc[2],
                announcement_detail_type=caption, file_urls=[doc[1]]
            ))
        return items

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
