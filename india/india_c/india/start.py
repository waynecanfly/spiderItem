import os


os.chdir("/root/spiderItem/india/india_c")
os.system("scrapy crawl BasicInfoBSE")
os.system("scrapy crawl BasicInfoNSE")