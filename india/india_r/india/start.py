import os
from Move_2_Nas import Move2Nas
from Initialization import Initialization


os.chdir("/root/spiderItem/india/india_r")
Initialization().InitializeMain2()
os.system("scrapy crawl downloadPdf_BSE_A")
os.system("scrapy crawl downloadExcel_BSE")
os.system("scrapy crawl downloadPdf_BSE_Qv2")
os.system("scrapy crawl downloadExcel_NSE")
os.system("scrapy crawl downloadZip_NSE")
try:
    Initialization().InitializeMain()
except:
    pass
Move2Nas().Move2NasMain("/data/spiderData/india", "/homes3/Inida/")
