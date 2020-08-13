import os
import re
import sys
import math
import json
import string
import shutil
import logging
import argparse
import traceback
from datetime import datetime as dt
from urllib.parse import unquote, urlparse, parse_qs
from concurrent.futures import as_completed, ThreadPoolExecutor

import requests
import pandas as pd
from bs4 import BeautifulSoup, NavigableString

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC


def get_logger(log_level=logging.INFO):
    filename = os.path.split(__file__)[-1]
    log = logging.getLogger(filename)
    log_level = logging.INFO
    log.setLevel(log_level)
    log_handler = logging.StreamHandler()
    log_formatter = logging.Formatter('%(levelname)s: %(asctime)s - %(name)s:%(lineno)d - %(message)s')
    log_handler.setFormatter(log_formatter)
    log.addHandler(log_handler)
    return log

log = get_logger(__file__)


def get_settings():
    with open("settings.json") as f:
        return json.load(f)


class FbScraper:

    def __init__(self, soup):
        self.soup = soup
        self.settings = get_settings()
        self.url = None
        self.chrome = None

    @property
    def post_date(self):
        try:
            return self.soup.find("span", class_="timestampContent", id=re.compile(r"js_\d+")).parent.attrs["title"]
        except:
            return ""

    @property
    def reactions(self):
        try:
            return self.soup.find_all("a", attrs={"data-testid": re.compile(r".*UFI2ReactionsCount.*", re.IGNORECASE)})[-1].next.next.text
        except:
            return ""

    @property
    def comments(self):
        try:
            return self.soup.find_all("a", string=re.compile(r"\scomments\s*$", re.IGNORECASE))[-1].text
        except:
            return ""

    @property
    def shares(self):
        try:
            return self.soup.find_all("a", attrs={"data-testid": re.compile(r".*UFI2SharesCount.*", re.IGNORECASE)})[-1].text
        except:
            return ""

    @property
    def title(self):
        try:
            return self.soup.find("meta", attrs={"property": "og:title"}).attrs["content"]
        except:
            return ""

    @property
    def description(self):
        try:
            return self.soup.find("meta", attrs={"name": "description"}).attrs["content"]
        except:
            return ""

    @property
    def views(self):
        try:
            return self.soup.find("span", string=re.compile(r"[\d,]+\sviews$", re.IGNORECASE)).text
        except:
            return ""

    @property
    def outbound_url(self):
        try:
            url = unquote(self.soup.find("a", attrs={"data-lynx-mode": "async"}, id=re.compile(r"u_0_\d[a-z]")).attrs["href"])
            return parse_qs(urlparse(url).query)['u'][0]
        except:
            return ""

    def scrape(self, row):
        return {
            "name": row["name"],
            "url": row["url"],
            "post_title": self.title,
            "post_description": self.description,
            "post_date": self.post_date,
            "no_of_reactions": self.reactions,
            "no_of_comments": self.comments,
            "no_of_shares": self.shares,
            "no_of_views": self.views,
            "outbound_url": self.outbound_url
        }


class FbManager:

    xlsx_input = "input.xlsx"
    xlsx_output = 'output.xlsx'

    def __init__(self):
        self.settings = get_settings()
        self.data = []

    def get_info(self, row):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        log_path = '/dev/null' if sys.platform == "linux" else "NUL"

        chrome = webdriver.Chrome(self.settings["driver_path"]["value"], chrome_options=options, service_log_path=log_path)
        chrome.get(row["url"])
        WebDriverWait(
            chrome, self.settings["page_load_timeout"]["value"]
        ).until(EC.presence_of_element_located((By.ID, "content_container")))
        soup = BeautifulSoup(chrome.page_source, "html.parser")
        chrome.close()

        soup.find('div', id="u_0_d").decompose()
        return FbScraper(soup).scrape(row)

    def get(self):
        count = 1
        rows = pd.read_excel(self.xlsx_input).fillna("").to_dict("records")

        workers = self.settings["workers"]["value"]
        with ThreadPoolExecutor(workers) as executor:      
            for info in executor.map(self.get_info, rows):
                self.data.append(info)

                if count % 5 == 0:
                    log.info("So far {} has been fetched ...".format(count))
                count += 1

    def save(self):
        data_exist = pd.read_excel(self.xlsx_output).to_dict("records") if os.path.exists(self.xlsx_output) else []
        df = pd.DataFrame(self.data + data_exist)
        df = df.drop_duplicates("url")

        urls_processed = list(df['url'])
        df_input = pd.read_excel(self.xlsx_input)
        df_unprocessed = df_input[~(df_input["url"].isin(urls_processed))]
        df_unprocessed = df_unprocessed.dropna()

        # Saving input file
        df_unprocessed.to_excel(self.xlsx_input, index=False)
        log.info("Fetched data has been removed in {} file".format(self.xlsx_input))

        # Saving output file
        df.to_excel(self.xlsx_output, engine="xlsxwriter", index=False)
        log.info("Fetched data has been stored in {} file".format(self.xlsx_output))


def main():
    start = dt.now()
    log.info("Script starts at: {}".format(start.strftime("%d-%m-%Y %H:%M:%S %p")))

    manager = FbManager()
    try:
        manager.get()
    except Exception as e:
        log.error(f"Error: {e}")
        traceback.print_exc()
    finally:
        manager.save()

    end = dt.now()
    log.info("Script ends at: {}".format(end.strftime("%d-%m-%Y %H:%M:%S %p")))
    elapsed = round(((end - start).seconds / 60), 4)
    log.info("Time Elapsed: {} minutes".format(elapsed))


if __name__ == "__main__":
    main()
