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

@dataclass
class SearchData:
    name: str = ""
    author: str = ""
    permalink: str = ""
    upvote_ratio: float = 0.0

    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            # Check string fields
            if isinstance(getattr(self, field.name), str):
                # If empty set default text
                if getattr(self, field.name) == '':
                    setattr(self, field.name, f"No {field.name}")
                    continue
                # Strip any trailing spaces, etc.
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())



class DataPipeline:
    
    def __init__(self, csv_filename='', storage_queue_limit=50):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.csv_filename = csv_filename
        self.csv_file_open = False
    
    def save_to_csv(self):
        self.csv_file_open = True
        data_to_save = []
        data_to_save.extend(self.storage_queue)
        self.storage_queue.clear()
        if not data_to_save:
            return

        keys = [field.name for field in fields(data_to_save[0])]
        file_exists = os.path.isfile(self.csv_filename) and os.path.getsize(self.csv_filename) > 0
        with open(self.csv_filename, mode='a', newline='', encoding='utf-8') as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)

            if not file_exists:
                writer.writeheader()

            for item in data_to_save:
                writer.writerow(asdict(item))

        self.csv_file_open = False
                    
    def is_duplicate(self, input_data):
        if input_data.name in self.names_seen:
            logger.warning(f"Duplicate item found: {input_data.name}. Item dropped.")
            return True
        self.names_seen.append(input_data.name)
        return False
            
    def add_data(self, scraped_data):
        if self.is_duplicate(scraped_data) == False:
            self.storage_queue.append(scraped_data)
            if len(self.storage_queue) >= self.storage_queue_limit and self.csv_file_open == False:
                self.save_to_csv()
                       
    def close_pipeline(self):
        if self.csv_file_open:
            time.sleep(3)
        if len(self.storage_queue) > 0:
            self.save_to_csv()

def get_scrapeops_url(url, location="us"):
    payload = {
        "api_key": API_KEY,
        "url": url,
        "country": location
    }
    proxy_url = "https://proxy.scrapeops.io/v1/?" + urlencode(payload)
    return proxy_url

#get posts from a subreddit
def get_posts(feed, limit=100, retries=3, data_pipline=None):
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            url = f"https://www.reddit.com/r/{feed}.json?limit={limit}"
            proxy_url = get_scrapeops_url(url)
            resp = requests.get(proxy_url, headers=headers)
            if resp.status_code == 200:
                success = True
                children = resp.json()["data"]["children"]
                for child in children:
                    data = child["data"]
                    
                    #extract individual fields from the site data
                    article_data = SearchData(
                        name=data["title"],
                        author=data["author_fullname"],
                        permalink=data["permalink"],
                        upvote_ratio=data["upvote_ratio"]
                    )
                    data_pipline.add_data(article_data)


            else:
                logger.warning(f"Failed response: {resp.status_code}")
                raise Exception("Failed to get posts")
        except Exception as e:
            logger.warning(f"Exeception, failed to get posts: {e}")
            tries += 1

#process an individual post
def process_post(post_object, retries=3, max_workers=5, location="us"):
    tries = 0
    success = False

    permalink = post_object["permalink"]
    r_url = f"https://www.reddit.com{permalink}.json"

    link_array = permalink.split("/")
    filename = link_array[-2].replace(" ", "-")


    comment_data = requests.get(r_url)
    comments = comment_data.json()

    if not isinstance(comments, list):
        return None
    

    comments_list = comments[1]["data"]["children"]

    while tries <= retries and not success:
        try:
            for comment in comments_list:
                if comment["kind"] != "more":
                    data = comment["data"]
                    comment_data = {
                        "name": data["author"],
                        "body": data["body"],
                        "upvotes": data["ups"]
                    }
                    print(f"Comment: {comment_data}")
                    success = True
        except Exception as e:
            logger.warning(f"Failed to retrieve comment:\n{e}")
            tries += 1
    if not success:
        raise Exception(f"Max retries exceeded {retries}")


#process a batch of posts
def process_posts(csv_file, max_workers=5, location="us"):
    with open(csv_file, newline="") as csvfile:
        reader = list(csv.DictReader(csvfile))
        for row in reader:
            process_post(row)
    

########### MAIN FUNCTION #############

if __name__ == "__main__":

    FEEDS = ["news"]
    BATCH_SIZE = 10

    AGGREGATED_FEEDS = []

    for feed in FEEDS:
        feed_filename = feed.replace(" ", "-")
        feed_pipeline = DataPipeline(csv_filename=f"{feed_filename}.csv")
        get_posts(feed, limit=BATCH_SIZE, data_pipline=feed_pipeline)
        feed_pipeline.close_pipeline()
        AGGREGATED_FEEDS.append(f"{feed_filename}.csv")

    for individual_file in AGGREGATED_FEEDS:
        process_posts(individual_file)
