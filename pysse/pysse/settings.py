# -*- coding: utf-8 -*-

# Scrapy settings for pysse project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

import os
import time
import logging

import pymysql

BOT_NAME = 'pysse'

SPIDER_MODULES = ['pysse.spiders']
NEWSPIDER_MODULE = 'pysse.spiders'

LOG_DATE, LOG_TIME = time.strftime('%Y-%m-%d %H-%M').split()
LOG_DIR = os.path.join('/home/liqiang/logs/sse', LOG_DATE)
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_FILE = os.path.join(LOG_DIR, LOG_TIME + '.log')
LOG_LEVEL = logging.INFO


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'pysse (+http://www.yourdomain.com)'
USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
]

# Configure database params
DBARGS = {
    'host': '10.100.4.99',
    'user': 'liqiang',
    'passwd': 'originp123',
    'charset': 'utf8mb4',
    'db': 'opd_common',
    'autocommit': True,
    'cursorclass': pymysql.cursors.DictCursor
}

FEED_EXPORT_ENCODING = 'utf-8'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

DOWNLOAD_WARNSIZE = 0
DOWNLOAD_DELAY = 0.5
DOWNLOAD_FAIL_ON_DATALOSS = False
DUPEFILTER_CLASS = 'scrapy.dupefilters.BaseDupeFilter'

FILES_STORE = '/data/spiderData/ssebond'
DOC_PATH = '/volume2/data/sse/{}/{}'

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'pysse.middlewares.PysseSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'pysse.middlewares.RandomUserAgentMiddleware': 543,
}

# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'pysse.pipelines.DefaultValuePipeline': 300,
    'pysse.pipelines.CompanyCodePipeline': 350,
    'pysse.pipelines.AnnounRenamePipeline': 400,
    'pysse.pipelines.AnnounDownloaderPipeline': 450,
    'pysse.pipelines.MySQLPipeline': 500,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
