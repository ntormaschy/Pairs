import INIT_STORE as I_S
import pandas as pd

def refresh_pair(pair, cnxn):

    sql = """
    SELECT 
        *
    FROM {}
    ORDER BY time DESC
    LIMIT 1
    """.format(pair)

    df = pd.read_sql(sql, cnxn)

    last_refresh = df['time'].values[0]
    df = I_S.get_historical_data(pair, I_S.PRICE_COMP, I_S.GRANULARITY, _from = last_refresh)

    return df 

def refresh_store():
    cnxn = I_S.connect_cloud_sql('PAIRS', I_S.GCLOUD_PWD, I_S.GCLOUD_IP,
        I_S.SSL_CA_PATH, I_S.SSL_CERT_PATH, I_S.SSL_KEY_PATH)
    pairs = I_S.get_stored_pairs()

    with cnxn.cursor() as cursor:
        for pair in pairs:
            df = refresh_pair(pair, cnxn)
            if df.shape[0] > 0:
                print("Inserting {} values into table".format(str(df.shape[0])))
                query = ("INSERT INTO {} (complete, volume, time, bid_o, bid_h, bid_l, bid_c, offer_o, offer_h, offer_l, offer_c) "
                    "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(pair))
                cursor.executemany(query, list(df.values.tolist()))
                print("{} updated Succesfully".format(pair))
            else:
                print("No values to update")
    
    cnxn.commit()
    cnxn.close()

if __name__ == "__main__":
    refresh_store()