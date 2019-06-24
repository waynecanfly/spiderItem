# -*- coding: utf-8 -*-
from ftplib import FTP
import os
import re


class Move2Nas(object):
    num = 0

    def Movefunc(self, dir_list, ftp):
        for temp in dir_list:
            fiscal_year = self.get_fiscal_year(temp)
            try:
                ftp.mkd("/homes/India/" + fiscal_year)
            except:
                pass
            self.num += 1
            self.uploadfile(ftp, "/homes/India/" + fiscal_year + "/" + temp, str(dir_list) + "/" + temp)
            print("已上传%s个文件到NAS服务器" % self.num)

    def get_fiscal_year(self, file_name):
        """获取财年"""
        pattern = re.compile("IND1.{4}(\d{4})")
        data = pattern.search(file_name)
        return data.group(1)

    def ftpconnect(self, host, username, password):
        """建立连接"""
        ftp = FTP()
        #ftp.set_debuglevel(2)
        ftp.connect(host, 21)
        ftp.login(username, password)
        print(ftp.getwelcome())
        #ftp.cwd('/homes/test_zx')
        #ftp.dir()
        return ftp

    def uploadfile(self, ftp, remotepath, localpath):
        """从本地上传文件到FTP"""
        bufsize = 1024
        fp = open(localpath, 'rb')
        ftp.storbinary('STOR ' + remotepath, fp, bufsize)
        ftp.set_debuglevel(0)
        fp.close()

    def Move2NasMain(self):
        ftp = self.ftpconnect("10.100.4.102", "admin", "originp123")
        dir_list = os.listdir("/data/OPDCMS/india/listed_company_update/pdf")
        Movefunc(dir_list, ftp)
        dir_list = os.listdir("/data/OPDCMS/india/listed_company_update/zip")
        Movefunc(dir_list, ftp)
        
        ftp.quit()
