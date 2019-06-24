#coding:utf-8
import shutil
import os


class Initialization(object):
    def InitializeMain(self):
        shutil.rmtree('D:\item\OPDCMS/report data update\india\data\pdf/full')
        shutil.rmtree('D:\item\OPDCMS/report data update\india\data\zip/full')
        print("*"*93)
        for i in range(2):
            print("*" + '\t'*23 + "*")
        print("*" + '\t'*10 + '初始化完成!' + '\t'*11 + "*")
        for i in range(2):
            print("*" + '\t'*23 + "*")
        print("*" * 93)
