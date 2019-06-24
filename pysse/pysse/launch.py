import os

from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

from utils.mover import move_nas

os.chdir('/root/spiderItem/pysse/pysse')

configure_logging()
runner = CrawlerRunner(get_project_settings())


@defer.inlineCallbacks
def crawl():
    yield runner.crawl('update_bond_list')
    yield runner.crawl('update_bond_info_daily')
    yield runner.crawl('update_bond_docs')
    yield runner.crawl('docs_mate')
    reactor.stop()


crawl()
reactor.run()

move_nas('/data/spiderData/ssebond', '/data/ssebond')
