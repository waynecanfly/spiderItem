import os

from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

from utils.mover import move_nas
from settings import FILES_STORE


os.chdir('/root/spiderItem/pycan/pycan')


configure_logging()
runner = CrawlerRunner(get_project_settings())


@defer.inlineCallbacks
def crawl():
    # yield runner.crawl('update_company_list')
    # yield runner.crawl('update_latest_ipo_date')
    # yield runner.crawl('update_company_docs')
    yield runner.crawl('docs_downloader')
    reactor.stop()


crawl()
reactor.run()

move_nas(FILES_STORE, '/homes/CAN')
