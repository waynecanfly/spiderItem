# -*- coding: utf-8 -*-
import os
from Move_2_Nas import Move2Nas
from Initialization import Initialization


os.chdir("/root/spiderItem/SouthAfrica")
Initialization().InitializeMain2()
os.system("scrapy crawl sfAll")
os.system("scrapy crawl sfFile")
try:
    Initialization().InitializeMain()
except FileNotFoundError:
    pass
Move2Nas().Move2NasMain("/data/spiderData/SouthAfrica", "/homes3/ZAF/")