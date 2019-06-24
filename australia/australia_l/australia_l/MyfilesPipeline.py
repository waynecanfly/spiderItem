# -*- coding: utf-8 -*-
import scrapy
import os
import requests
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import DropItem
from scrapy.utils.project import get_project_settings
from australia_l.items import AustraliacompanyupdateItem


class MyfilesPipeline(FilesPipeline):
    FILES_STORE = get_project_settings().get("FILES_STORE")

    def get_media_requests(self, item, info):
        if item["doc_source_url"] is not None:
            file_url = item["doc_source_url"]
            if isinstance(item, AustraliacompanyupdateItem):
                response = requests.head(file_url)
                if response.status_code == 302:
                    file_url = response.headers["Location"]
            try:
                yield scrapy.Request(file_url)
            except:
                item["is_downloaded"] = 0

    def item_completed(self, results, item, info):
        """下载完成之后，重命名文件之类的处理，文件路径在results 里，具体results数据结构用pdb看一下就可以了"""
        file_paths = [x["path"] for ok, x in results if ok]
        if not file_paths:
            raise DropItem("Item contains no images")
        try:
            os.rename(self.FILES_STORE + "/" + file_paths[0], self.FILES_STORE + "/" + item["report_id"] + ".pdf")
        except:
            item["is_downloaded"] = 0
        item["pdf_path"] = self.FILES_STORE + "/" + item["pdf_name"]
        return item
