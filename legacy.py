from selenium import webdriver
import time
import pandas as pd
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
import random


chrome_options = Options()
#chrome_options.add_argument("--headless")
#chrome_options.add_argument("--disable-gpu")
options = Options()
ua = UserAgent()
userAgent = ua.random
chrome_options.add_argument(f'user-agent={userAgent}')
wd = webdriver.Chrome(executable_path='C:\Webdrivers\chromedriver.exe',options=chrome_options)
def wait(n):
    return time.sleep(random.uniform(1,int(n)))
url = 'https://login.yotpo.com/?product=sms'


def export():
    #get url
    wd.get(url)
    wait(10)
    #input username and password
    username = wd.find_elements_by_xpath('//*[@id="yo-sign-in"]/div/div/div/form/div[1]/input')[0]
    password = wd.find_elements_by_xpath('//*[@id="yo-sign-in"]/div/div/div/form/div[2]/input')[0]
    username.send_keys()
    wait(3)
    password.send_keys()
    wait(3)
    #login button click
    wd.find_elements_by_xpath('//*[@id="login-button"]')[0].click()
    wait(5)
    wd.find_elements_by_xpath('//*[@id="menu"]/li[9]/a/span[2]')[0].click()
    wait(5)
    #export button click
    wd.find_elements_by_xpath('//*[@id="layout-parent"]/div/div/div/div[3]/div/div/div/div/div[2]/div/table/tbody/tr[1]/td[5]/div/button[1]')[0].click()
    wait(3)
