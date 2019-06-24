import os


os.chdir('/root/spiderItem/china/china_update_infor')
os.system("scrapy crawl UpdatePro_shenzhen_basicinfo")
os.system("scrapy crawl UpdatePro_shanghai_basicinfo")
