# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

import random


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


class OverrunMateMiddleware(object):
    """重发因请求频繁导致的无正常响应请求"""

    retry_count = 3

    def process_response(self, request, response, spider):
        if (response.xpath("//center[contains(text(), '查詢過於頻繁')]") or
                response.xpath('//body[contains(text(), "頁面無法執行")]')):
            request.meta['is_downloaded'] = False
        else:
            request.meta['is_downloaded'] = True
        request.meta['doc_url'] = '{}?{}'.format(
            request.url, request.body.decode())

        return response
