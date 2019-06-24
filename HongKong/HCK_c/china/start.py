import os


os.chdir('/root/spiderItem/HongKong/HCK_c')
os.system("scrapy crawl HCK_information")
os.system("scrapy crawl HCK_infor_for_web")
