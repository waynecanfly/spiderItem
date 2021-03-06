# -*- coding: utf-8 -*-
import scrapy
import os
#import requests
import time
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem
from scrapy.utils.project import get_project_settings
from china.items import ChinaIntroItem_sh, ChinaIntroItem_sh_non


class MyfilesPipeline(FilesPipeline):
    FILES_STORE = get_project_settings().get("FILES_STORE")

    def get_media_requests(self, item, info):
        if item["doc_source_url"] is not None:
            file_url = item["doc_source_url"]
            # if isinstance(item, ChinaIntroItem_sh) or isinstance(item, ChinaIntroItem_sh_non):
            #     time.sleep(1)
            yield scrapy.Request(file_url)
            item["is_downloaded"] = 0
            """
            response = requests.head(file_url)
            if response.status_code == 302:
                file_url = response.headers["Location"]
            """

    def item_completed(self, results, item, info):
        """下载完成之后，重命名文件之类的处理，文件路径在results 里，具体results数据结构用pdb看一下就可以了"""
        file_paths = [x["path"] for ok, x in results if ok]
        print(file_paths, "="*100)
        if not file_paths:
            raise DropItem("Item contains no file")
        # try:
        if item["doc_type"] == "excel":
            os.rename(self.FILES_STORE + "/" + file_paths[0], self.FILES_STORE + "/" + item["report_id"] + ".xls")
        else:
            os.rename(self.FILES_STORE + "/" + file_paths[0], self.FILES_STORE + "/" + item["report_id"] + ".pdf")
        # except:
        #     item["is_downloaded"] = 0
        item["file_path"] = self.FILES_STORE + "/" + item["file_name"]
        return item
