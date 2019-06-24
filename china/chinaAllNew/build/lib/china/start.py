from scrapy import cmdline
import os
from ScriptDir.Initialization import Initialization
from ScriptDir.Move_2_Nas import Move2Nas


#cmdline.execute("scrapy crawl UpdatePro_shanghai".split())
#cmdline.execute("scrapy crawl UpdatePro_shenzhen".split())
#os.system("scrapy crawl shanghai_download_spider")
os.system("scrapy crawl shenzhen_download_spider")
Initialization().InitializeMain()
Move2Nas().Move2NasMain()
