# -*- coding: utf-8 -*-
from datetime import datetime, timedelta


a = datetime(2007, 1, 1)
while a <= datetime.now():
    b = a + timedelta(days=30)
    qt = str(a).split(" ")[0] + "&endDate=" + str(b).split(" ")[0]
    a = b + timedelta(days=1)
    print(qt)