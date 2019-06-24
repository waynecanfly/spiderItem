"""下载、更新公司英文公司信息"""

import re
import json

import scrapy
import pymysql
from scrapy import FormRequest
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError
from twisted.web._newclient import ResponseNeverReceived

from ..items import ProfileDefinitionItem, ProfileDetailItem


class UpdateCompanyInfoSpider(scrapy.Spider):
    name = 'update_company_info_en'

    start_urls = ['http://englishdart.fss.or.kr/dsbc001/main.do']

    search_url = 'http://englishdart.fss.or.kr/dsbc001/search.ax'
    detail_url = 'http://englishdart.fss.or.kr/dsbc002/main.do'
    more_url = 'http://englishdart.fss.or.kr/dsbc002/{}.ax'

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UpdateCompanyInfoSpider, cls).from_crawler(
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
                select code, security_code from company where \
                country_code_listed='KOR'\
            """)
            records = cursor.fetchall()
        conn.close()

        self.companies = {it['security_code']: it['code'] for it in records}

    def spider_closed(self, spider):
        self.logger.info(
            'Closing spider %s...', spider.name,
        )

    def parse(self, response):
        """使用字母表搜索公司信息"""
        for i in range(27):
            yield FormRequest.from_response(
                response,
                formname='searchForm',
                formdata={'searchIndex': str(i), 'currentPage': '1'},
                callback=self.parse_companies,
                errback=self.errback_scraping,
                meta={'first': True, 'current': 1}
            ).replace(url=self.search_url)

    def parse_companies(self, response):
        entries = response.xpath('//table/tbody/tr')
        for i in range(1, len(entries), 2):  # 有标题栏及空行，需跳过
            for j in [3, 6]:  # 一行两组公司信息，按code所在列解析
                code = entries[i].xpath(
                    './td[{}]/text()'.format(j)).extract_first('').strip()
                if code in self.companies:
                    href = entries[i].xpath(
                        './td[{}]/a/@href'.format(j - 2)).extract_first()
                    yield response.follow(
                        href,
                        callback=self.parse_company_profiles,
                        errback=self.errback_scraping,
                        meta={'company': self.companies[code]}
                    )

        if 'first' in response.meta:  # 确定总页数
            last_page = int(response.xpath(
                "//a[@href='#last']/@onclick").re_first(r'(?<=\()\d+'))
        else:
            last_page = response.meta['last']

        if response.meta['current'] < last_page:  # 搜索下一页
            currenct_request = response.request
            current_request_body = currenct_request.body.decode()
            next_page = response.meta['current'] + 1
            body = re.sub(
                r'(?<=currentPage=)\d+', str(next_page), current_request_body
            ).encode()
            yield currenct_request.replace(
                body=body,
                meta={'current': next_page, 'last': last_page}
            )

    def parse_company_profiles(self, response):
        """解析弹框公司信息页内容"""
        parent = ProfileDefinitionItem(
            name='company_information_en_kor',
            display_label='Company infomation', data_type='string'
        )
        entries = response.xpath('//table/tbody/tr')
        for it in entries:
            label = it.xpath('./th/text()').extract_first().strip()
            value = it.xpath('string(./td)').extract_first().strip()
            yield ProfileDetailItem(
                name=label.lower().replace(' ', '_') + '_en_kor',
                display_label=label, value=value,
                company_code=response.meta['company'], parent=parent
            )

        # 查询所需更多信息
        symbol, name = response.xpath(
            "//a[contains(@onclick, 'moreDetail')]/@onclick"
        ).re(r"'(.*?)'")
        yield FormRequest(
            self.detail_url,
            formdata={'selectKey': symbol, 'textCrpNm': name},
            callback=self.parse_company_details,
            errback=self.errback_scraping,
            meta={'company': response.meta['company'], 'selectKey': symbol}
        )

    def parse_company_details(self, response):
        """解析More detail information页面"""
        parent = ProfileDefinitionItem(
            name='more_detail_information_en_kor',
            display_label='More detail information', data_type='string'
        )

        entries = response.xpath(
            "//table[contains(@summary, 'detailed')]/tbody/tr")
        for it in entries:
            items = it.xpath('child::*')
            for i in range(0, len(items), 2):
                label = items[i].xpath('text()').extract_first('').strip()
                value = items[i + 1].xpath('text()').extract_first('').strip()
                yield ProfileDetailItem(
                    name=label.lower().replace(' ', '_') + '_en_kor',
                    display_label=label, company_code=response.meta['company'],
                    value=value, parent=parent
                )

        direct_captions = ['Main Businesses', 'Sales Composition']
        for caption in direct_captions:
            text = response.xpath(
                "//p[text()='{}']/following-sibling::div/text()".format(
                    caption)).extract()
            value = '\n'.join(line.strip() for line in text)
            yield ProfileDetailItem(
                name=caption.lower().replace(' ', '_') + '_en_kor',
                display_label=caption, company_code=response.meta['company'],
                value=value, parent=parent
            )

        indirect_captions = ['Changes in Capital', 'Major Stockholders']
        for caption in indirect_captions:
            func = response.xpath(
                "//p[contains(text(), '{}')]/a/@onclick".format(
                    caption)).re_first(r'\w+(?=\()')
            yield FormRequest(
                self.more_url.format(func),
                formdata={'selectKey': response.meta['selectKey']},
                callback=self.parse_more_detaild,
                errback=self.errback_scraping,
                meta={
                    'company': response.meta['company'],
                    'caption': caption, 'parent': parent}
            )

    def parse_more_detaild(self, response):
        """将该信息通过json序列化存储"""
        main_headers = response.xpath('//table/thead/tr[1]/th')
        sub_headers = response.xpath('//table/thead/tr[2]/th/text()').extract()
        sub_headers = [it.strip() for it in sub_headers]

        count = 0
        title = []
        for m in main_headers:  # 处理表头（跨列）
            header = m.xpath('text()').extract_first()
            try:
                cols = int(m.xpath('@colspan').extract_first())
                for i in range(count, count + cols):
                    title.append(header + '|' + sub_headers[i])
                count += cols
            except TypeError:
                title.append(header)

        values = []
        for tr in response.xpath('//table/tbody/tr'):
            vals = []
            for td in tr.xpath('./td'):
                val = td.xpath('text()').extract_first()
                vals.append(val.strip() if val else val)
            values.append(vals)

        value = json.dumps({
            'title': title,
            'values': values
        })

        name = response.meta['caption'].lower().replace(' ', '_') + '_en_kor'
        yield ProfileDetailItem(
            name=name, display_label=response.meta['caption'],
            company_code=response.meta['company'], value=value,
            parent=response.meta['parent']
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
