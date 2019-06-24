#coding: utf-8
import zipfile
import os


class Decompression(object):
    def __init__(self):
        self.fail_unzip_list = []
        self.num = 0

    def unzip_func(self, file_name, path, report_id):
        """unzip zip file"""
        zip_file = zipfile.ZipFile(file_name)
        for names in zip_file.namelist():
            zip_file.extract(names, path)
            os.chdir(path)
            os.rename(names, report_id)
        zip_file.close()

    def get_file_name(self):
        """获取文件名"""
        path = "D:\item\OPDCMS/report data update\india\data\pdf"
        dir_list = os.listdir("D:\item\OPDCMS/report data update\india\data\zip")
        for temp in dir_list:
            self.num += 1
            print("正在解压第%s个" % self.num)
            zip_name = "D:\item\OPDCMS/report data update\india\data\zip/" + temp
            report_id = str(zip_name).split(".")[0] + ".pdf"
            try:
                self.unzip_func(zip_name, path, report_id)
            except:
                self.fail_unzip_list.append(temp)
        print("不能解压的压缩包有%s" % self.fail_unzip_list)

    def UnzipMain(self):
        self.get_file_name()
