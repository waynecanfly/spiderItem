# -*- coding: utf-8 -*-
# from datetime import datetime, timedelta
#
#
# with open("/root/spiderItem/italy/record.txt", "r") as f:
#     data = f.read()
#     year = int(data.split("@")[0])
#     month = int(data.split("@")[1])
#     day = int(data.split("@")[-1])
#     a = datetime(year, month, day) + timedelta(days=30)
#     if a >= datetime.now():
#         print(a, datetime.now())
#         newRecord = str(datetime.now()).split(" ")[0].split("-")[0] + "@" + \
#                     str(datetime.now()).split(" ")[0].split("-")[1] + "@" + \
#                     str(datetime.now()).split(" ")[0].split("-")[-1]
#         with open("/root/spiderItem/italy/record.txt", "w") as g:
#             g.write(newRecord)

import time


a = -799120800
time_local = time.localtime(a)
dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
print(dt)