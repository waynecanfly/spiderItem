import os


os.chdir("/root/spiderItem/india/india_l")
#这里要执行companylist_BSEv2脚本，但是爬虫初始化会自动执行，此处不用再执行
os.system("scrapy crawl companylist_NSE")
os.system("scrapy crawl indiaExcuteThird")
os.system("scrapy crawl BasicInfoBSE")
os.system("scrapy crawl BasicInfoNSE")