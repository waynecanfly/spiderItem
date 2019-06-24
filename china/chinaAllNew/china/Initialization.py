#coding:utf-8
import shutil
import os


class Initialization(object):
    def InitializeMain(self):
        shutil.rmtree('/data/spiderData/china/full')
        print("*"*93)
        for i in range(2):
            print("*" + '\t'*23 + "*")
        print("*" + '\t'*10 + '初始化完成!' + '\t'*11 + "*")
        for i in range(2):
            print("*" + '\t'*23 + "*")
        print("*" * 93)

    def InitializeMain2(self):
        shutil.rmtree('/data/spiderData/china')
        os.mkdir("/data/spiderData/china")
