# This code collects the list of friends for a given list of Twitter users.
# The list of friends is the set of users that each of the given users is following on Twitter.
#
# Copyright 2022 Nir Lotan
# Licensed under the Apache License, Version 2.0 (the "License"); You may not use this file except in compliance with the License.

import os
import tweepy
import time
import pandas as pd
import click
import logging
from importlib import reload
from tqdm import trange
from datetime import datetime


max_token = 1

# Default global value for the tokens index to start with
global_token_index = 0

#
def aux_sleep():
    """
    This function introduces a delay to comply with the rate limits specified in the Twitter Developer API documentation
    Args: None
    :return: None
    """
    sleep_time = 15*600 # sleep for 15 minutes
    # show sleep progress
    for i in trange(sleep_time,
                    desc = f"sleep for {sleep_time/600} minutes"):
        time.sleep(0.1)


# Initial connection to Twitter
def connect_to_twitter(tokens_df: pd.DataFrame, token_index: int = 0, proxy = None):
    """
    This function initializes the connection to Twitter using the tokens specified in the tokens.csv file
    :param token_index: specifies the index of the token to use, supporting rotating through multiple tokens
    :return: Twitter API interface
    """

    consumer_key        = tokens_df.iloc[token_index]['consumer_key']
    consumer_secret     = tokens_df.iloc[token_index]['consumer_secret']
    access_token        = tokens_df.iloc[token_index]['access_token']
    access_token_secret = tokens_df.iloc[token_index]['access_token_secret']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    if proxy is not None:
        api = tweepy.API(auth,proxy="http://proxy-chain.intel.com:911")
    else:
        api = tweepy.API(auth)
    
    return api

@click.command()
@click.option('--users', default="data/users_to_collect.csv.gz", help='File with the list of twitter_ids in the provided format')
@click.option('--tokens', default="tokens_file.csv", help='A Twitter developer API token list, formatted as the provided sample')
@click.option('--proxy', help='Proxy to use if needed.')
def main_function(users :str, tokens :str, proxy :str):

    global global_token_index
    global max_token

    # Unset proxy, in case there is a system proxy
    os.system("unset http_proxy")
    os.system("unset https_proxy")

    # Open Twitter Developer API tokens file
    tokens_df = pd.read_csv(tokens)
    max_token = tokens_df.shape[0]

    api = connect_to_twitter(tokens_df, global_token_index, proxy)

    # Create log file
    now = datetime.now() # current date and time
    date_time = now.strftime("%Y%m%d_%H%M%S")


    # Go over the list of users and collect
    df = pd.read_csv(users, compression='gzip')

    # Check if an output folder exists
    if not os.path.exists("collect_friends/output"):
        # Create the folder
        os.makedirs("collect_friends/output")

    outfile = open(f"output/friends_collected_{date_time}.csv","a+")
    deleted_file = open(f"output/deleted_or_private_users_{date_time}.csv","a+")
    reload(logging)
    logging.basicConfig(filename= f"output/collection{date_time}.log",
                        filemode='w', format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',level=logging.DEBUG)


    for uid in df['twitter_id']:
        # Making sure user IDs are in the right format
        uid = str(int(uid))

        for attempt in range(10):
            try:
                print (f"user#: {uid}")
                friends = sorted(api.get_friend_ids(user_id=uid))
                if len(friends) > 0:
                    for item in friends:
                        outfile.write(f"{uid},{item}\n")
                    logging.debug(f"Completed user#:{uid}")
                else:
                    logging.debug(f"user#:{uid} has no friends")
                    deleted_file.write(f"{uid}\n")

            except tweepy.errors.TweepyException as e:
                if 'Rate limit exceeded' in e.api_messages:
                    global_token_index = (global_token_index + 1) % (max_token + 1)
                    if global_token_index != 0:
                        print(f"Rate limit exceeded. Retrying with token #{global_token_index}")
                        api = connect_to_twitter(tokens_df, global_token_index, proxy)
                    else:
                        print(f"Rate limit exceeded. Wait and retry {10-attempt} more times...")
                        time.sleep(1)
                        aux_sleep()
                    continue #retry
                else:
                    print (f"error: {e.api_errors[0]}")
                    deleted_file.write(f"{uid}\n")
                    logging.debug(f"user#:{uid} Error:{e.api_codes}")
                    break # we failed not because of rate limit
            # We succeeded
            break
        #completed all attempts

if __name__ == '__main__':
    main_function()