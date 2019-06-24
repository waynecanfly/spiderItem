# -*- coding: utf-8 -*-
import os
# from Move_2_Nas import Move2Nas
# from Initialization import Initialization


os.chdir("/root/spiderItem/italy")
# Initialization().InitializeMain2()
os.system("scrapy crawl italyAll")
# os.system("scrapy crawl italyFile")
# try:
#     Initialization().InitializeMain()
# except FileNotFoundError:
#     pass
# Move2Nas().Move2NasMain("/data/spiderData/italy", "/homes3/Italy/")