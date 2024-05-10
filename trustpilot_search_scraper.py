import os
import time
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict


def get_scrapeops_url(url, location):
    payload = {'api_key': SCRAPEOPS_API_KEY, 'url': url, 'country': location}
    proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
    return proxy_url


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dataclass
class SearchData:
    name: str = ""
    stars: float = 0
    rating: float = 0
    num_reviews: int = 0

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
            logger.warn(f"Duplicate item found: {input_data.name}. Item dropped.")
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



def generate_url_list(keyword, last_page_number=0):
    ## Sets Max Page Depth Scraper Should Paginate Too
    if MAX_PAGE_DEPTH > 0 and last_page_number > MAX_PAGE_DEPTH:
        last_page_number = MAX_PAGE_DEPTH

    ## Generate Request List
    if last_page_number > 1:
        for page_number in range(1, last_page_number):
                request_list.append({
                    'keyword': keyword,
                    'url': f'https://www.trustpilot.com/search?query={keyword}&page={page_number+1}'
                })
    else:
        request_list.append({
                    'keyword': keyword,
                    'url': f'https://www.trustpilot.com/search?query={keyword}'
                })
    

def scrape_search_results(request_data):
    request_list.remove(request_data)
    url = request_data.get('url')
    keyword = request_data.get('keyword')

    ## Check If Url Already Scraped
    if url in scraped_urls:
        return
    scraped_urls.append(url)
    
    for _ in range(NUM_RETRIES):
        try:
            scrapeops_proxy_url = get_scrapeops_url(url)
            response = requests.get(scrapeops_proxy_url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code in [200, 404]:
                
                ## Extract Data
                soup = BeautifulSoup(response.text, 'html.parser')
                script_tag = soup.find('script', id='__NEXT_DATA__')
                if script_tag:
                    json_data = json.loads(script_tag.contents[0])
                    business_units = json_data['props']['pageProps']['businessUnits']
                    for business in business_units:
                        try:

                            ## Extract Data
                            search_data = SearchData(
                                name = business.get('displayName', ''),
                                stars = business.get('stars', 0),
                                rating = business.get('trustScore', 0),
                                num_reviews = business.get('numberOfReviews', 0)
                                
                                ## Add below fields...
                                #location
                                #url to company website
                                #url to trustpilot page
                                #category
                            )

                            ## Add To Data Pipeline
                            data_pipeline.add_data(search_data)
                            #logger.info(f"Successfully parsed data from: {url}")
                            
                            
                        except Exception as e:
                            logger.error(f"Error parsing search result: {e}")
                            continue

                else:
                    logger.warning(f"No JSON blob found on page: {url}")

                
                ## Pagination
                if last_found_page_dict[keyword] == False:
                    last_pagination_link = soup.find('a', {'data-pagination-button-last-link': 'true'})
                    if last_pagination_link:
                        last_page_url = last_pagination_link['href']
                        last_page_number = int(last_page_url.split('page=')[-1].split('&')[0])
                        last_found_page_dict[keyword] = True
                        generate_url_list(keyword, last_page_number)   

                # Break Retries
                break
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}")




def start_scrape(keyword_list):
    ## Generate List of URLs For Each Keyword
    for keyword in keyword_list:
        generate_url_list(keyword)
        last_found_page_dict[keyword] = False

    ## Start Concurrent Scraper
    while len(request_list) > 0:
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            executor.map(scrape_search_results, request_list)
        time.sleep(3)



# Internal Request Tracking
request_list = []
scraped_urls = []
last_found_page_dict = {}

# Retries & Concurrency
NUM_RETRIES = 3
NUM_THREADS = 10
MAX_PAGE_DEPTH = 2
SCRAPEOPS_API_KEY = 'YOUR_API_KEY'


if __name__ == "__main__":
    logger.info(f"Job starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ['vpn']

    ## Job Processes
    data_pipeline = DataPipeline(csv_filename='search_data.csv')
    start_scrape(keyword_list)
    data_pipeline.close_pipeline()
    logger.info(f"Job complete.")
