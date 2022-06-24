import json 

ENV_SETTING = "OANDA_DEMO"
KEY_PATH = "./PRIVATE/KEYS.txt"
KEY_DICT = json.load(open(KEY_PATH))
API_KEY = KEY_DICT[ENV_SETTING]

print(API_KEY)

