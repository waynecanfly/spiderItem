import os
from india.ScriptDir.Initialization import Initialization
from india.ScriptDir.unzip_download_files import Decompression
from india.ScriptDir.Move_2_Nas import Move2Nas


# os.system("scrapy crawl downloadPdf_BSE_A")
# os.system("scrapy crawl downloadExcel_BSE")
# os.system("scrapy crawl downloadPdf_BSE_Q")
os.system("scrapy crawl downloadExcel_NSE")
os.system("scrapy crawl downloadZip_NSE")
# Initialization().InitializeMain()
# Decompression().UnzipMain()
# Move2Nas.Move2NasMain()