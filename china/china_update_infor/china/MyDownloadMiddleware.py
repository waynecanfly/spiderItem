import requests


class IP(object):

    def get_ip(self):
        proxy_url = "http://api.ip.data5u.com/dynamic/get.html?order=1556f0687ba5dde702aa91e1573b0feb&sep=3"
        response = requests.get(proxy_url)
        ip = response.text
        return ip

    def test_ip(self, ip):
        url = "http://www.sse.com.cn"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36"
        }
        proxy = {
            'http': ip
        }
        try:
            req = requests.get(url, headers=headers, proxies=proxy, timeout=3)
            StatusCode = req.status_code
        except:
            StatusCode = 408
        return StatusCode

    def process_request(self, request, spider):
        while True:
            ip = self.get_ip().replace("\n", "")
            StatusCode = self.test_ip(ip)
            if StatusCode == 200:
                request.meta["proxy"] = "http://" + ip
                break
