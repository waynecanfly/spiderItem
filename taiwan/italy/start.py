import os


"""
taiwanlistzh下载台湾中文列表，已做更新功能
taiwanlisten下载台湾英文列表，已做更新功能
info_enAll首次存量下载台湾英文基本信息
info_en为下载台湾增量基本信息而生

以下若要更新需覆盖
taiwanFileAllv3下载英文财报，原网站最新只到2018年3月份
info_zhAll下载中文基本信息："重要子公司基本資料"，"重要子公司異動說明"， "被投資控股公司基本資料" （文件）
info_zh下载中文基本信息："公司基本資料" （格式化）
info_zh2下载中文基本信息："歷年變更登記"（文件） 需要界面化才能获取数据，需要windows系统
"""

os.chdir('/root/spiderItem/taiwan/italy/spiders')
os.system("python3 taiwanlistzh.py")
# os.system('python3 info_zhAll.py')
os.chdir('/root/spiderItem/taiwan/italy/script2')
os.system("python3 taiwanlisten.py")
os.system('python3 info_en.py')
# os.system("python3 taiwanFileAllv3.py")
# os.system('python3 info_zh.py')
# os.system('python3 info_zh2.py')
