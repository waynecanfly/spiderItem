# -*- coding: utf-8 -*-
from ftplib import FTP
import os
import re


class Move2Nas(object):
    num = 0

    def get_fiscal_year(self, file_name):
        """获取财年"""
        try:
            pattern = re.compile("CHN1\d{4}")
            data = pattern.search(file_name)
            return data.group()
        except:
            return "0000"

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

    def Move2NasMain(self, LocalDir, NasDir):
        ftp = self.ftpconnect("10.100.4.102", "admin", "originp123")
        dir_list = os.listdir(LocalDir)
        for temp in dir_list:
            # fiscal_year = self.get_fiscal_year(temp)
            fiscal_year = "0000"
            try:
                ftp.mkd(NasDir + fiscal_year)
            except:
                pass
            self.num += 1
            self.uploadfile(ftp, NasDir + fiscal_year + "/" + temp, LocalDir + "/" + temp)
            print("已上传%s个文件到NAS服务器" % self.num)
        ftp.quit()
