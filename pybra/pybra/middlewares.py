# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

import random
import json

import requests

from .settings import USER_AGENT_LIST


class RandomUserAgentMiddleware(object):
    def process_request(sel, request, spider):
        user_agent = random.choice(USER_AGENT_LIST)
        request.headers.setdefault('User-Agent', user_agent)


class ProxyMiddleware(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings['API_URL'])

    def __init__(self, api_url):
        self.api_url = api_url
        self.proxy_ip = None
        self.total_urls = 0

    def process_request(self, request, spider):
        if self.total_urls % 10 == 0:
            self.proxy_ip = self.get_proxy_ip()

        if self.proxy_ip:
            request.meta['proxy'] = 'http://' + self.proxy_ip
            request.meta['ip'] = self.proxy_ip
        else:
            request.meta['ip'] = 'localhost'

    def get_proxy_ip(self):
        try:
            response = requests.get(self.api_url, timeout=5)
            jres = json.loads(response.text)
            if jres['success']:
                ip = jres['data'][0]['ip']
                port = jres['data'][0]['port']
                return ip + ':' + str(port)
        except requests.exceptions.Timeout:
            pass
