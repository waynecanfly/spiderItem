import os
from Move_2_Nas import Move2Nas
from Initialization import Initialization


os.chdir('/root/spiderItem/HongKong/HCK_l')
Initialization().InitializeMain2()
os.system("scrapy crawl HCK_Execute1")
os.system("scrapy crawl HCK_Execute2")
os.system("scrapy crawl HCK_pdf_spider")
os.system("scrapy crawl HCK_information")
try:
    Initialization().InitializeMain()
except FileNotFoundError:
    pass
Move2Nas().Move2NasMain("/data/spiderData/HongKong", "/homes3/HongKong/")
