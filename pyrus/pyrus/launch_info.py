import os

from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

os.chdir('/root/spiderItem/pyrus/pyrus')


configure_logging()
runner = CrawlerRunner(get_project_settings())


@defer.inlineCallbacks
def crawl():
    yield runner.crawl('update_company_info_en')
    reactor.stop()


crawl()
reactor.run()
