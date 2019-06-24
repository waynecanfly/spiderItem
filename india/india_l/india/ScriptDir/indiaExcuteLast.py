import scrapy
from india.Initialization import Initialization
from india.unzip_download_files import Decompression
from india.Move_2_Nas import Move2Nas


class indiaExcuteLast(scrapy.Spider):
    name = "indiaExcuteLast"
    allowed_domains = "baidu.com"

    def start_requests(self):
        Initialization().InitializeMain()
        Decompression().UnzipMain()
        Move2Nas().Move2NasMain()
        url = "https://www.baidu.com"
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        print("The last part is complete!!!")