import os

from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

from utils.mover import move_nas

os.chdir('/root/spiderItem/pyusa/pyusa')

configure_logging()
runner = CrawlerRunner(get_project_settings())


@defer.inlineCallbacks
def crawl():
    yield runner.crawl('update_company_list')
    yield runner.crawl('update_company_docs')
    reactor.stop()


crawl()
reactor.run()

move_nas('/data/lq/usa/reports', '/data/usa')
move_nas('/data/lq/usa/announcements', '/data/usa_announcements')
