# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

import os
import random
from hashlib import md5

import pymysql
import requests
from scrapy import FormRequest, signals
from requests.exceptions import Timeout, ConnectionError

from .settings import USER_AGENT_LIST, IMGS_DIR, DBARGS


class RandomUserAgentMiddleware(object):

    def __init__(self, user_agents):
        self.user_agents = user_agents

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['USER_AGENT_LIST'])

    def process_request(self, request, spider):
        request.headers.setdefault(
            'User-Agent', random.choice(self.user_agents)
        )


class CAPTCHAMiddleware(object):

    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        pass

    def process_response(self, request, response, spider):
        if spider.name == 'docs_downloader':
            if response.headers[b'Content-Type'].decode().startswith('text'):

                # 获取Cookies用于下载验证码图片
                cookies = self.get_cookies(request, response)
                codes = self.get_codes(response, cookies)
                if codes:
                    action = response.xpath('//form/@action').extract_first()
                    action_url = response.urljoin(action)
                    return FormRequest(
                        action_url,
                        formdata={'code': codes},
                        callback=request.callback,
                        cookies=cookies,
                        headers={'Referer': request.meta['url']},
                        meta={
                            'code': codes,
                            'url': request.meta['url'],
                            'table': request.meta['table'],
                            'report_id': request.meta['report_id'],
                            'doc_type': request.meta['doc_type'],
                            'fiscal_year': request.meta['fiscal_year']
                        }
                    )
                else:
                    return response

        return response

    def get_cookies(self, request, response):
        try:
            cookies = dict(map(
                lambda it: it.strip().split('='),
                request.headers[b'Cookie'].decode().split(';')
            ))
        except KeyError:  # 从response中获取cookies
            cookies = dict(map(
                lambda it: it.decode().split(';')[0].split('='),
                response.headers.getlist(b'Set-Cookie')
            ))

        return cookies

    def get_codes(self, response, cookies):
        try:
            codes = []
            for index, it in enumerate(response.xpath('//img')):
                url = response.urljoin(it.xpath('@src').extract_first())
                img_resp = requests.get(
                    url, headers={'User-Agent': USER_AGENT_LIST[0]},
                    cookies=cookies, timeout=60
                )

                img_hash = md5(img_resp.content).hexdigest()
                for it in self.captchas:
                    if img_hash == it['dHash']:
                        codes.append(it['str_name'])
                        break
                else:
                    with open(
                        os.path.join(IMGS_DIR, '{}.jpg'.format(img_hash)), 'wb'
                    ) as f:
                        f.write(img_resp.content)
                    codes.append(None)

            if all(codes):
                return ''.join(codes)

        except (Timeout, ConnectionError):
            return None

    def spider_opened(self, spider):
        conn = pymysql.connect(**DBARGS)
        with conn.cursor() as cursor:
            cursor.execute("""\
                select str_name, dHash from canada_captcha_hash where \
                user_create='lq'\
            """)
            self.captchas = cursor.fetchall()
