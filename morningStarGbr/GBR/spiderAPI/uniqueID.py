# -*- coding: utf-8 -*-
import datetime
import random


def uniqueIDMaker():
    time_id = str(datetime.datetime.now()).split(".")[-1]
    random_id1 = str(random.randrange(0, 9))
    random_id2 = str(random.randrange(0, 9))
    unique_id = time_id + random_id1 + random_id2
    return unique_id