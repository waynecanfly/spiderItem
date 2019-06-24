import os

from china.chinaAllNew.china.Initialization import Initialization
from china.chinaAllNew.china.Move_2_Nas import Move2Nas

os.chdir('/root/spiderItem/china/chinaAllNew')
Initialization().InitializeMain2()#初始化，清除上次下载的文件
os.system("scrapy crawl china_updateAll_l")#下载所有公司列表
os.system('scrapy crawl chinaExecute2')#与数据库原有公司列表对比，从而得出新上市公司，如有则入库
os.system("scrapy crawl china_updateAll_r")#更新深交所公告等信息
os.system('scrapy crawl shanghai_download_spiderV2')#更新上交所财报及非财报
os.system('scrapy crawl shenzhen_download_spiderV2')#更新深交所财报及非财报
os.system("scrapy crawl china_updateAll_c")#下载新上市公司基本信息
try:
    Initialization().InitializeMain()#删除full文件夹
except FileNotFoundError:
    pass
Move2Nas().Move2NasMain("/data/spiderData/china", "/homes3/China/")#将所下载的文件上传至NAS服务器
