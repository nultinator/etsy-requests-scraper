import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(keyword, location, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    url = f"https://www.etsy.com/search?q={formatted_keyword}&ref=pagination&"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code != 200:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
            soup = BeautifulSoup(response.text, "html.parser")
            
            div_cards = soup.find_all("div", class_="wt-height-full")

            result_count = 0
            last_listing = ""
            for div_card in div_cards:
                title = div_card.find("h3")
                if not title:
                    continue
                name = title.get("title")
                a_tag = div_card.find("a")
                listing_id = a_tag.get("data-listing-id")
                if listing_id == last_listing:
                    continue
                link = a_tag.get("href")
                stars = 0.0
                has_stars = div_card.find("span", class_="wt-text-title-small")
                if has_stars:
                    stars = float(has_stars.text)
                currency = "n/a"
                currency_holder = div_card.find("span", class_="currency-symbol")
                if currency_holder:
                    currency = currency_holder.text

                prices = div_card.find_all("span", class_="currency-value")
                if len(prices) < 1:
                    continue
                current_price = prices[0].text
                original_price = current_price
                if len(prices) > 1:
                    original_price = prices[1].text

                search_data = {
                    "name": name,
                    "stars": stars,
                    "url": link,
                    "price_currency": currency,
                    "listing_id": listing_id,
                    "current_price": current_price,
                    "original_price": original_price
                }
                print(search_data)
                
                result_count+=1
                last_listing = listing_id                

            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries+=1

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


        
if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["coffee mug"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        scrape_search_results(keyword, LOCATION, retries=MAX_RETRIES)

    logger.info(f"Crawl complete.")