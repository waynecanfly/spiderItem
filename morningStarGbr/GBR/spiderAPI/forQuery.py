# -*- coding: utf-8 -*-
import json


def get_params():
    with open("/root/zx/item/GBR/GBR/spiderAPI/query.txt", "r") as f:
        text = f.read()
        if text.startswith(u'\ufeff'):
            text = text.encode('utf8')[3:].decode('utf8')
        data = json.loads(text)
        return data
