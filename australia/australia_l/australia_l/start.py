# -*- coding: utf-8 -*-
import os
from Move_2_Nas import Move2Nas
from Initialization import Initialization


os.chdir('/root/spiderItem/australia/australia_l')
Initialization().InitializeMain2()
os.system("scrapy crawl australiaExcuteFirst")
os.system("scrapy crawl DownloadPdf-ASX")
os.system("scrapy crawl DownloadPdf-NSX")
os.system("scrapy crawl BasicInfo_NSX")
try:
    Initialization().InitializeMain()
except FileNotFoundError:
    pass
Move2Nas().Move2NasMain("/data/spiderData/australia", "/homes3/Australia/")




