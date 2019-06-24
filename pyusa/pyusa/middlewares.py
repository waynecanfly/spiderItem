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
