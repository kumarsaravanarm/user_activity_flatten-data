import json
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
import logging
import configparser

logging.basicConfig(filename='flatten_pipeline.log',
                    filemode='a',
                    format='[%(asctime)s] [%(msecs)d] [%(name)s]: %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

def extract_data(DB_CONFIG, query):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            try:
                # executing the select query in user_activity_log table
                cur.execute(query)
                fetch_data = cur.fetchall()
                # extracting data from user_activity_log table and converting into dict
                df = pd.DataFrame(fetch_data, columns=['user_id', 'activity_data']).to_dict(orient='records')

                return df
            except Exception as e:
                print(f"There is an error on executing query as {e}")
                return pd.DataFrame()


def flatten_data(user_id, activity_data):
    # flattening user and activity data
    city = activity_data.get("city", "").strip() or None
    device = activity_data.get("device", "").strip() or None
    country = activity_data.get("country", "").strip() or None

    action_list = activity_data.get("actions", [])
    
    if action_list != []:
        action_data = []
        for action_value in action_list:
            device_details = {'user_id': user_id, 'device': device, 'city': city, 'country': country}

            device_details['action_type'] = action_value.get('action_type', '')
            device_details['action_target'] = action_value.get('action_target', '')
            device_details['action_timestamp'] = action_value.get('action_timestamp', datetime(2026, 1, 1))
            action_data.append(device_details)

    activity_data = pd.DataFrame(list(action_data))
    final_data = activity_data[['user_id', 'device', 'city', 'country', 'action_type', 'action_target', 'action_timestamp']]    
    return final_data

def insert_flatten_data(DB_CONFIG, flatten_df):
    # convert dataframe to tuple
    user_activity_data = tuple(flatten_df.to_numpy())
    
    # query for inserting data
    insert_query = """
    INSERT INTO master.flattened_user_activity
    (user_id, device, city, country, action_type, action_target, action_timestamp, processed_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # executing data as batch
            execute_batch(cur, insert_query, user_activity_data, page_size=100)
            conn.commit()
            logging.info("Flatten data inserted in DB")

def main(DB_CONFIG):

    # Select query to read the data from user_activity_log 
    select_query = """
    SELECT user_id, activity_data
    FROM master.user_activity_log
    WHERE user_id IS NOT NULL AND activity_data IS NOT NULL;
    """

    activity_list = extract_data(DB_CONFIG, select_query)
    logging.info("User data extracted from DB")

    final_df = pd.DataFrame()
    for activity in activity_list:

        user_data = activity['user_id']
        activity_data = activity['activity_data']

        if activity_data['actions'] != []:
            flatten_dataset = flatten_data(user_data, activity_data)
            final_df = pd.concat([final_df, flatten_dataset], axis=0, ignore_index=True)
    final_df['processed_at'] = datetime.now()
    logging.info("Flatten user data completed.")

    insert_flatten_data(DB_CONFIG, final_df)
    logging.info(f"{'-'*50} Script completed for {datetime.now()} {'-'*50}")


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('config.ini') # Read the file

    db_host = config['database']['host']
    db_name = config['database']['db_name']
    user_name = config['database']['user_name']
    password = config['database']['password']
    db_port = int(config['database']['port'])

    DB_CONFIG = {
                    "host": db_host,
                    "database": db_name,
                    "user": user_name,
                    "password": password,
                    "port": db_port
                }
    logging.info(f"{'-'*50} Script started for {datetime.now()} {'-'*50}")

    main(DB_CONFIG)