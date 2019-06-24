# -*- coding: utf-8 -*-
# import numpy as np
#
#
# a = np.arange(15).reshape(3, 5)
# b = np.arange(1,16).reshape(3, 5)
# print(a)
# print(b)
# print(a.ndim)
# print(a.shape)
# print(a.size)
# print(a.dtype)
# print(a.itemsize)
# print(a.data)
# a = np.array([
#     (1, 2, 3),
#     (4, 5, 6),
#     (7, 8, 9)
# ])
# print(a)
# print(a.shape)
# print(a.ndim)
# print(np.arange(6).reshape(2, 3))
# print(np.zeros((3, 3), dtype=int))
# print(np.ones((3,4), dtype=int))
# print(np.full((3,4), 3))
# print(np.eye(4,k=-1,dtype=int))
# print(np.random.random(6).reshape(2, 3))
# print(a[2,3])
# print(a[1:3, :])
# print(np.argsort(a))


# -*- coding: utf-8 -*-
import time
import random
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains


a = 0
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
driver = Chrome(executable_path="/root/chrome/drive/chromedriver", chrome_options=chrome_options)

driver.get('https://passport.xiami.com/?spm=a1z1s.6843761.226669510.9.WDpd6x&redirectURL=https://www.xiami.com')
login_page = driver.find_element_by_id('J_LoginSwitch')
login_page.click()
time.sleep(2)
account_input = driver.find_element_by_id('account')
passwd_input = driver.find_element_by_id('password')
account_input.send_keys('13632262371')
passwd_input.send_keys('5229193')
time.sleep(1)
button = driver.find_element_by_id('nc_1_n1z')
action = ActionChains(driver)
action.click_and_hold(button).perform()
time.sleep(0.4)
action.reset_actions()


def get_track(distance):
    track = []
    current = 0
    mid = distance * 3 / 4
    t = 0.2
    v = 6
    while current < distance:
        if current < mid:
            a = 160
        else:
            a = -12
        v0 = v
        v = v0 + a * t
        move = v0 * t + 1 / 2 * a * t * t
        current += move
        track.append(round(move))
    return track


track = get_track(208)
for temp in track:
    a += temp
    if a > 208:
        temp = 208 - (a - temp)
    action.move_by_offset(temp, 0).perform()
    time.sleep(random.uniform(0.8, 1.2))
    print(driver.find_element_by_id('nc_1_n1z').get_attribute('style'))
time.sleep(1)
print(driver.find_element_by_id('nc_1_n1z').get_attribute('style'))
driver.find_element_by_id("submit").click()
time.sleep(2)
print(driver.find_element_by_id('error').text)
driver.close()
