import os
import json 
import requests

IS_DEMO = True
ENV_SETTING = "OANDA_DEMO"
ACT_SETTING = "OANDA_DEMO_ACT_NUM"
KEY_PATH = r"..\PRIVATE\KEYS.txt"

KEY_DICT = json.load(open(KEY_PATH))
API_KEY = KEY_DICT[ENV_SETTING]
ACT_NUM = KEY_DICT[ACT_SETTING]

global REQUEST_HEADERS
REQUEST_HEADERS = {
    "Content-Type" : "application/json",
    "Authorization" : "Bearer {}".format(API_KEY)
}

if not IS_DEMO:
    ACT_URL = "https://api-fxtrade.oanda.com/v3/accounts/"
else:
    ACT_URL = "https://api-fxpractice.oanda.com/v3/accounts/"

def handle_response(res):
    status_code = int(res.status_code)

    if status_code == 200:
        data = res.json()
        return data
    else:
        print("ERROR: Status Code {}".format(str(status_code)))
        print(res.text)
        return None
    


def get_tradeable_pairs(account_num):
    url = ACT_URL + "/{}/instruments".format(str(account_num))

    try:
        res = requests.get(url, headers = REQUEST_HEADERS)
    except ConnectionError as e:
        print(e)
        return None 

    data = handle_response(res)

    if data == None:
        print("Failed to fetch pairs")
        return None
    else:
        print(data)

get_tradeable_pairs(ACT_NUM)
