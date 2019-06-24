"""
1.spider bond 负责下载巨潮上深市债券列表及基本信息，由于数据量不是很大，更新策略是有则根据bond_code更新数据，无则插入新数据
2.spider add_szse_bond 负责下载深交所上债券列表及部分信息（深交所基本信息不全）并和巨潮上的比对去重后插入数据库，更新原则同上
3.spider fileProUpdate 负责下载并更新债券公告及问询函 监管措施 纪律处分。 策略是拿到各个类型下的最新时间的url 去和原网页从上
    往下比对，当遇到相同的url后不再往后遍历，插入数据库并做了一层doc_source_url是否存在的判断（避免最新时间下的url有多个，
    会出现下载重复的情况）。
"""
import os
from Move_2_Nas import Move2Nas
from Initialization import Initialization


os.chdir('/root/spiderItem/shenzhenBond')
Initialization().InitializeMain2()
os.system("scrapy crawl bond")
os.system("scrapy crawl add_szse_bond")
os.system("scrapy crawl fileProUpdate")
try:
    Initialization().InitializeMain()
except FileNotFoundError:
    pass
Move2Nas().Move2NasMain("/data/spiderData/shenzhenBond", "/homes3/ChinaSecurities/")
