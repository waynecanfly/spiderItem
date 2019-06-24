# -*- coding: utf-8 -*-
import random
from HCK_l.settings import PROXIES
from fake_useragent import UserAgent


class MyRequest(object):
    def process_request(self, request, spider):
        proxy = random.choice(PROXIES)
        request.meta["proxy"] = "http://" + proxy["ip_port"]


class MyUserAgent(object):
    def process_request(self, request, spider):
        ua = UserAgent()
        agent = ua.random
        request.headers.setdefault("User-Agent", agent)
