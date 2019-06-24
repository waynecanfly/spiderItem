import os
from Move_2_Nas import Move2Nas
from Initialization import Initialization


os.chdir("/root/spiderItem/germany/germany_l")
Initialization().InitializeMain2()
os.system("scrapy crawl FrankfurtCompanyList")
os.system("scrapy crawl germanyExcute2")
os.system("scrapy crawl Frankfurtpdf")
os.system("scrapy crawl FrankfurtBasicInfo")
try:
    Initialization().InitializeMain()
except FileNotFoundError:
    pass
Move2Nas().Move2NasMain("/data/spiderData/germany", "/homes3/Germany/")
