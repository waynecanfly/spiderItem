#coding:utf-8
import shutil
import os


class Initialization(object):
    def InitializeMain(self):
        shutil.rmtree('/data/OPDCMS/india/listed_company_update/zip/full')
        shutil.rmtree('/data/OPDCMS/india/listed_company_update/pdf/full')
        print("*"*93)
        for i in range(2):
            print("*" + '\t'*23 + "*")
        print("*" + '\t'*10 + '初始化完成!' + '\t'*11 + "*")
        for i in range(2):
            print("*" + '\t'*23 + "*")
        print("*" * 93)

    def InitializeMain2(self):
        shutil.rmtree('/data/OPDCMS/india/listed_company_update/zip')
        shutil.rmtree('/data/OPDCMS/india/listed_company_update/pdf')
        shutil.rmtree('/data/OPDCMS/india/listed_company_update/csv')
        os.mkdir('/data/OPDCMS/india/listed_company_update/zip')
        os.mkdir('/data/OPDCMS/india/listed_company_update/pdf')
        os.mkdir('/data/OPDCMS/india/listed_company_update/csv')
