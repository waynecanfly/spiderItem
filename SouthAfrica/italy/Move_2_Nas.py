# -*- coding: utf-8 -*-
import pymysql
from ftplib import FTP
import os


class Move2Nas(object):
    num = 0

    def __init__(self):
        self.conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA", charset="utf8")
        self.cursor = self.conn.cursor()

    def get_fiscal_year(self, file_name):
        """获取财年"""
        sql = "select fiscal_year from financial_statement_index where report_id=%s"
        self.cursor.execute(sql, file_name.split(".")[0])
        result = self.cursor.fetchone()
        if result:
            return str(result[0])
        else:
            return "0000"

    def ftpconnect(self, host, username, password):
        """建立连接"""
        ftp = FTP()
        ftp.connect(host, 21)
        ftp.login(username, password)
        print(ftp.getwelcome())
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
            fiscal_year = self.get_fiscal_year(temp)
            try:
                ftp.mkd(NasDir + fiscal_year)
            except:
                pass
            self.num += 1
            self.uploadfile(ftp, NasDir + fiscal_year + "/" + temp, LocalDir + "/" + temp)
            print("已上传%s个文件到NAS服务器" % self.num)
        ftp.quit()
