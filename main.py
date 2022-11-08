import json
import pandas as pd
from datetime import datetime
import time
from requests import *
from threading import Thread
from twilio.rest import Client
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
AUTH_TOKEN = os.environ.get('AUTH_TOKEN')
ACCOUNT_SID = os.environ.get('ACCOUNT_SID')
PHONE = os.environ.get('PHONE')
TWILIO_PHONE = os.environ.get('TWILIO_PHONE')

starting_new_file = True
running = True
run_counter = 0
date_file = f"{time.strftime('%d%m%Y')}"
size = 500000000
time_between_request = 60


def send_text(to, message):
    client = Client(ACCOUNT_SID, AUTH_TOKEN)
    message_info = client.messages \
        .create(body=message, from_=TWILIO_PHONE, to=f'+1{to}')
    return message_info.sid


def get_all_osrs_item():
    """Requests all the osrs items and sorts into useable format and saves to file."""
    all_item_mapping = 'https://prices.runescape.wiki/api/v1/osrs/mapping'
    r = get(url=all_item_mapping).json()
    df = pd.DataFrame(r)
    clean_df = df[['id', 'name', 'members', 'limit']].set_index('id').sort_values(by='id', ascending=True)
    """saves to a file."""
    # clean_df.to_csv("cleaned_osrs_all_item_list.csv")


def get_item_price_check():
    """Requests latest price of all osrs items
    Formats that into a pd df, cleans it and removes all NaN entries
    """
    pc_all_item = 'https://prices.runescape.wiki/api/v1/osrs/latest'
    r = get(url=pc_all_item).json()['data']
    pc_df = pd.DataFrame(r).transpose().reset_index().rename(columns={'index': 'id'})
    new_column = ['id', 'highTime', 'lowTime', 'high', 'low']
    pc_df = pc_df[new_column + (pc_df.columns.drop(new_column).tolist())]
    pc_df = pc_df.fillna(0)
    return pc_df


def utc_to_local(utc_datetime):
    """Gets utc time and returns local"""
    exchange = (datetime.fromtimestamp(time.time()) - datetime.utcfromtimestamp(time.time())) + \
               (datetime.fromisoformat(utc_datetime))
    return exchange

def save_file(df):
    global date_file, size
    file_path = f'data/price_data_{date_file}.json'
    file_size = os.stat(file_path).st_size
    if file_size > size:
        with open(file_path, 'w+') as f:
            json.dump(df, f)
        date_file = f"{time.strftime('%d-%m-%Y')}"
        text_message = 'A new OSRS file was created.'
        send_text(to=PHONE, message=text_message)
        return date_file
    else:
        with open(file_path, 'w+') as f:
            json.dump(df, f)


def check_starting_file():
    """This checks if were starting with a new file or its already been used. """
    global starting_new_file, date_file
    file_path = f'price_data_{date_file}.json'
    try:
        if os.stat(file_path).st_size == 0:
            pass
        else:
            with open(file_path) as file:
                df = json.load(file)
            starting_new_file = False
    except FileNotFoundError:
        with open(file_path, 'w+') as f:
            pass
        df = pd.read_csv('data/cleaned_osrs_all_item_list.csv', index_col=False).set_index('id').fillna(0).to_dict('index')
    df_sf = [df, starting_new_file]
    return df_sf


def sorting_monster(pc_df):
    """Takes parameter of all the latest prices of every osrs item and creates a new file combining latest
    price check data and the item info.
     """
    global starting_new_file, run_counter, date_file
    check_sf = check_starting_file()
    df = check_sf[0]
    starting_new_file = check_sf[1]

    """Sorts through the newest price checked data for all items."""
    for index, row in pc_df.iterrows():
        high_utx_time = row['highTime']
        low_utx_time = row['lowTime']
        high_price = row['high']
        low_price = row['low']

        """Since json writes keys as strings 
        below we change the main id accordingly so that the dataframe can be read"""
        if starting_new_file:
            id_item = int(row['id'])
        else:
            id_item = str(row['id'])

        """Below takes the local time that has been filtered by our function and formats into date and local time"""
        pc_date = utc_to_local(str(datetime.utcfromtimestamp(high_utx_time))).strftime('%d-%m-%Y')
        time_stamp_local = utc_to_local(str(datetime.utcfromtimestamp(high_utx_time))).strftime('%I:%M:%S %p')

        """Below first check is the item is located in the main df of all items"""
        if id_item in df:
            """Below checks if price data has been created"""
            if 'price data' in df[id_item]:
                """Below checks to see if the current date is in the dict"""
                if pc_date in df[id_item]['price data']['date']:
                    """Below we check if the time stamp is the same"""
                    if time_stamp_local in df[id_item]['price data']['date'][pc_date]['minute pc']:
                        continue
                    else:
                        df[id_item]['price data']['date'][pc_date]['minute pc'][time_stamp_local] = {
                            'highTime': high_utx_time,
                            'lowTime': low_utx_time,
                            'high': high_price,
                            'low': low_price
                        }
                else:
                    df[id_item]['price data']['date'][pc_date] = {
                        'minute pc': {time_stamp_local: {'highTime': high_utx_time,
                                                         'lowTime': low_utx_time,
                                                         'high': high_price,
                                                         'low': low_price
                                                         }}}
            else:
                if id_item == id_item:
                    df[id_item]['price data'] = {
                        'date': {pc_date: {'minute pc': {time_stamp_local: {'highTime': high_utx_time,
                                                                            'lowTime': low_utx_time,
                                                                            'high': high_price,
                                                                            'low': low_price
                                                                            }}}}}
        else:
            continue
    """Saves the sorted the data counts to see how many times its ran"""
    save_file(df=df)
    run_counter += 1
    print(f'\nProgram has run {run_counter} times.')
    """Check if its a new file. If it is we set to false."""
    if starting_new_file:
        starting_new_file = False


def user_input():
    """Stops the while loop from running on the users input"""
    global running
    stop = input('To end program type: stop')
    if stop == 'stop':
        running = False
        print('saving all your data...')
    return running


def start():
    """This runs the whole program"""
    global date_file, time_between_request
    t = Thread(target=user_input)
    t.start()
    while running:
        sorting_monster(pc_df=get_item_price_check())
        time.sleep(time_between_request)


start()
try:
    text_message = 'Osrs price checker has stopped for some reason.'
    send_text(to=PHONE, message=text_message)
except:
    print("ERROR failed to send message. Check your API keys.")
