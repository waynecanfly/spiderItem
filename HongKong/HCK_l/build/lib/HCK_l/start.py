from scrapy import cmdline
import os


# os.system("scrapy crawl HCK_pdf_spider")
# os.system("scrapy crawl HCK_pdf_for_loss")
# os.system("scrapy crawl HCK_infor_for_web")
# os.system("scrapy crawl HCK_get_all_company_list")
# os.system("scrapy crawl HCK_Execute1")
# os.system("scrapy crawl HCK_Execute2")
# os.system("scrapy crawl HCK_information")
cmdline.execute("scrapy crawl HCK_information".split())