import os
import json 
import time
import requests
import pandas as pd
import mysql.connector

from mysql.connector.constants import ClientFlag

IS_DEMO = True
ENV_SETTING = "OANDA_DEMO"
ACT_SETTING = "OANDA_DEMO_ACT_NUM"
KEY_PATH = r"..\PRIVATE\KEYS.txt"

KEY_DICT = json.load(open(KEY_PATH))
API_KEY = KEY_DICT[ENV_SETTING]
ACT_NUM = KEY_DICT[ACT_SETTING]

SSL_CA_PATH = r"..\PRIVATE\server-ca.pem"
SSL_CERT_PATH = r"..\PRIVATE\client-cert.pem"
SSL_KEY_PATH = r"..\PRIVATE\client-key.pem"
GCLOUD_IP = KEY_DICT['GOOGLE_CLOUD_SQL_IP']
GCLOUD_PWD = KEY_DICT['GOOGLE_CLOUD_SQL_PWD']

global REQUEST_HEADERS
REQUEST_HEADERS = {
    "Content-Type" : "application/json",
    "Authorization" : "Bearer {}".format(API_KEY)
}

PAIRS_STORED = ['AUD_CAD', '']

if not IS_DEMO:
    ACT_URL = "https://api-fxtrade.oanda.com/v3/accounts/"
    INST_URL = "https://api-fxtrade.oanda.com/v3/instruments/"
else:
    ACT_URL = "https://api-fxpractice.oanda.com/v3/accounts/"
    INST_URL = "https://api-fxpractice.oanda.com/v3/instruments/"

def connect_cloud_sql(db, pwd, host, ssl_ca, ssl_cert, ssl_key):
    config = {
        'database' : db,
        'user': 'root',
        'password': pwd,
        'host': host,
        'client_flags': [ClientFlag.SSL],
        'ssl_ca': ssl_ca,
        'ssl_cert': ssl_cert,
        'ssl_key': ssl_key
    }
    cnxn = mysql.connector.connect(**config)
    return cnxn

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
    
    rows = []
    cols = ['name', 'type', 'displayName', 'displayPrecision', 'marginRate', 'longRate', 'ShortRate']
    instruments = data['instruments']

    for instrument in instruments:
        row = [instrument['name'], instrument['type'], instrument['displayName'], instrument['displayPrecision'],
                instrument['marginRate'], instrument['financing']['longRate'], instrument['financing']['shortRate']]
        rows.append(row)

    pairs_df = pd.DataFrame(rows)
    pairs_df.columns = cols

    cnxn = connect_cloud_sql('PAIRS', GCLOUD_PWD, GCLOUD_IP, SSL_CA_PATH, SSL_CERT_PATH, SSL_KEY_PATH)
    cursor = cnxn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS TRADEABLE_PAIRS ("
               "name VARCHAR(64),"
               "type VARCHAR(64),"
               "displayName VARCHAR(64),"
               "displayPrecision INT(2),"
               "marginRate FLOAT(24),"
               "longRate FLOAT(24),"
               "ShortRate FLOAT(24) )")
    cursor.execute("SELECT * FROM TRADEABLE_PAIRS")
    out = cursor.fetchall()
    if len(out) >0:
        print("Tradeable Pairs Already Created")
        for row in out[0:5]:
            print(row)
    else:  
        query = ("INSERT INTO TRADEABLE_PAIRS (name, type, displayName, displayPrecision, marginRate, longRate, ShortRate) "
            "values (%s, %s, %s, %s, %s, %s, %s)")

        cursor.executemany(query, list(pairs_df.values.tolist()))
        print("Tradeable Pairs Table Created")
        
    cnxn.commit() 
    cnxn.close() 

def get_stored_pairs():
    sql = "SELECT name FROM TRADEABLE_PAIRS WHERE marginRate < .05"
    cnxn = connect_cloud_sql('PAIRS', GCLOUD_PWD, GCLOUD_IP, SSL_CA_PATH, SSL_CERT_PATH, SSL_KEY_PATH)
    pairs_df = pd.read_sql(sql, cnxn)

    return pairs_df['name'].values
    cnxn.close() 

def get_historical_data(pair, priceComp, granularity, count):
    url = INST_URL + pair + "/candles?count=" + str(count) + "&price=" + priceComp + "&granularity=" + granularity

    try:
        res = requests.get(url, headers = REQUEST_HEADERS)
    except ConnectionError as e:
        print(e)
        return None 

    data = handle_response(res)
    data_df = pd.json_normalize(data, record_path=['candles'])
    data_df = data_df[data_df['complete'] ==  True]
    data_df['time'] = data_df['time'].astype('datetime64[s]')
    return data_df

def init_store():
    pairs = get_stored_pairs()
    cnxn = connect_cloud_sql('PAIRS', GCLOUD_PWD, GCLOUD_IP, SSL_CA_PATH, SSL_CERT_PATH, SSL_KEY_PATH)

    with cnxn.cursor() as cursor:
        for pair in pairs:
            time.sleep(1)
            print(pair)
            df = get_historical_data(pair,'BA','H1',5000)
            print("Dataframe of size {} pulled".format(df.shape))
            
            print("Creating table")
            cursor.execute("CREATE TABLE IF NOT EXISTS {} ("
                "complete BOOLEAN,"
                "volume INT(32),"
                "time timestamp,"
                "bid_o FLOAT(24),"
                "bid_h FLOAT(24),"
                "bid_l FLOAT(24),"
                "bid_c FLOAT(24),"
                "offer_o FLOAT(24),"
                "offer_h FLOAT(24),"
                "offer_l FLOAT(24),"
                "offer_c FLOAT(24)" ")".format(pair))
            cursor.execute("SELECT * FROM {}".format(pair))
            out = cursor.fetchall()

            if len(out) == 0:
                print("Inserting values into table")
                query = ("INSERT INTO {} (complete, volume, time, bid_o, bid_h, bid_l, bid_c, offer_o, offer_h, offer_l, offer_c) "
                    "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(pair))
                cursor.executemany(query, list(df.values.tolist()))
                print("{} Created Succesfully".format(pair))

    cnxn.commit()
    cnxn.close()

#get_tradeable_pairs(ACT_NUM)
#get_stored_pairs()
init_store()