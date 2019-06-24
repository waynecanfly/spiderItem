import requests
import time
from lxml import etree
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options


create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
create_time = str(create_time).replace("-", "").replace(" ", "").replace(":", "")
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)
url = "http://www.hkex.com.hk/Products/Securities/Equities?sc_lang=en"
Heads = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0'
}
driver.get(url)
time.sleep(2)
body = driver.page_source
# response = requests.get(url, headers=Heads)
# html = etree.HTML(response.text)
html = etree.HTML(body)
xls_link = "http://www.hkex.com.hk" + html.xpath('//div[@class="newsletter"]//li[@class="ls-process-ql__item"]/a/@href')[0]
req = requests.get(xls_link, headers=Heads)
# req.encoding = "gbk"
with open("/data/OPDCMS/HCK/company_list/HCK_" + create_time + ".xls", "ab") as f:
    f.write(req.content)
driver.quit()