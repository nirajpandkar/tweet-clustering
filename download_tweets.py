import json
import glob
from tqdm import tqdm
import traceback
import time
import os
import pandas as pd

import logging
logging.basicConfig(filename='tweets.log', encoding='utf-8', level=logging.INFO)


import tweepy


# import credentials
with open("credentials.json", "r") as infile:
    creds = json.load(infile)

auth = tweepy.OAuthHandler(creds["consumer_key"], creds["consumer_secret"])
auth.set_access_token(creds["access_token"], creds["access_token_secret"])

api = tweepy.API(auth, wait_on_rate_limit=True)

# Actual downloading script

# Parameters
sleep_time = 60*15            # seconds
n_ids_per_request = 100    # Twitter can take 100 tweet ids per lookup_status request
output_dir = "output"
os.system("mkdir -p " + output_dir)

for input_dir in sorted(glob.glob("parent_dir/*")):
    output_file_name = input_dir.split("/")[-1]
    
    dat_files_path = os.path.join(input_dir, "*.dat")

    log_dir = os.path.join("logs", input_dir)

    os.system("mkdir -p " + log_dir)
    with open(os.path.join(output_dir, output_file_name + ".txt"), 'w', encoding='utf-8') as output_file:
        dat_files = sorted(glob.glob(dat_files_path))
        
        for dat_file in tqdm(dat_files):
            logging.info(f"Reading from dat file: {dat_file}...\n")
            df = pd.read_csv(dat_file, delimiter="\t", header=None)
            list_ids = df[0].values

            logging.info(f"Number of tweets: {len(list_ids)}\n")
            for i in tqdm(range(0, len(list_ids), n_ids_per_request)):
                try:
                    list_statuses = api.lookup_statuses(list(list_ids)[i:i+n_ids_per_request])
                    for status in list_statuses:
                        json_object = status._json
                        json.dump(json_object, output_file)
                        output_file.write("\n")
                except Exception:
                    logging.error(traceback.format_exc())
                    with open(os.path.join(log_dir, "remaining_ids.txt"), "a") as outfile:
                        outfile.write("\n")
                        outfile.write("\n".join([str(ele) for ele in list(list_ids)[i:i+n_ids_per_request]]))
                    
                    logging.info("Going to sleep for 60*15 seconds")
                    time.sleep(sleep_time)
                    logging.info("Done sleeping! Continuing...")
    
            with open(os.path.join(log_dir, "dat_files_completed.txt"), "a") as outfile:
                outfile.write(dat_file + "\n")