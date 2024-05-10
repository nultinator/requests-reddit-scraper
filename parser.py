import requests
from urllib.parse import urlencode
import csv, json, time
import logging, os
from dataclasses import dataclass, field, fields, asdict
from concurrent.futures import ThreadPoolExecutor


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.3'}
proxy_url = "https://proxy.scrapeops.io/v1/"
API_KEY = "YOUR-SUPER-SECRET-API-KEY"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#get posts from a subreddit
def get_posts(feed, retries=3):
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            url = f"https://www.reddit.com/r/{feed}.json"
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                success = True
                children = resp.json()["data"]["children"]
                for child in children:
                    data = child["data"]
                    
                    #extract individual fields from the site data
                    name = data["title"]
                    author = data["author_fullname"]
                    permalink = data["permalink"]
                    upvote_ratio = data["upvote_ratio"]

                    #print the extracted data
                    print(f"Name: {name}")
                    print(f"Author: {author}")
                    print(f"Permalink: {permalink}")
                    print(f"Upvote Ratio: {upvote_ratio}")
                    
            else:
                logger.warning(f"Failed response: {resp.status_code}")
                raise Exception("Failed to get posts")
        except Exception as e:
            logger.warning(f"Exeception, failed to get posts: {e}")
            tries += 1
    

########### MAIN FUNCTION #############

if __name__ == "__main__":

    FEEDS = ["news"]

    for feed in FEEDS:
        get_posts(feed)
        
