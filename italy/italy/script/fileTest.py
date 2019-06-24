# -*- coding: utf-8 -*-
import requests


headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36"}
url = "https://www.borsaitaliana.it/documenti/documenti.htm?filename=116983.pdf"
response = requests.get(url, headers=headers)
print(response.text)