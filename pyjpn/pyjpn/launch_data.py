import os

from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

from utils.mover import move_nas
from settings import FILES_STORE

os.chdir('/root/spiderItem/pyjpn/pyjpn')


configure_logging()
runner = CrawlerRunner(get_project_settings())


@defer.inlineCallbacks
def crawl():
    yield runner.crawl('update_edinet_company_list')
    yield runner.crawl('update_edinet_company_reports')
    yield runner.crawl('update_edinet_company_announces')
    reactor.stop()


crawl()
reactor.run()

move_nas(FILES_STORE, '/homes/JPN_NEW')
