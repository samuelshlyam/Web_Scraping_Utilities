from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import re
import os
import datetime
import logging
import time
import csv
from bs4 import BeautifulSoup
from fastapi import FastAPI, BackgroundTasks
import uvicorn
import ray
from typing import Union

from fastapi import FastAPI

app = FastAPI()






@ray.remote
class PageExpander:

    def __init__(self,brand_id,url,output_dir):
        options = webdriver.ChromeOptions()
        options.add_argument("--auto-open-devtools-for-tabs")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        options.add_argument("start-maximized")
        options.add_argument("--headless=new")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")

        # Construct the full directory path
        self.output_dir=output_dir
        self.driver = webdriver.Chrome(options=options)
        self.brand_id=brand_id
        self.url=url
        self.data=fetch_settings()[brand_id]
        self.load_data()
        path = url.split("//")[-1]  # Remove protocol and get the path
        path = re.sub(r'[<>:"\\|?*]', '_', path)
        path_parts = path.split("/")
        logging_dir = os.path.join(output_dir, *path_parts)
        os.makedirs(logging_dir, exist_ok=True)
        log_filename = path_parts[-1] + ".log" if path_parts[-1] else path_parts[-2] + ".log"
        logging_file_path = os.path.join(logging_dir, log_filename)
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(logging_file_path),
                logging.StreamHandler()
            ])

        self.logger = logging.getLogger(__name__)
        self.logger.info("This is a log message from the gg script")

    def start(self):
        if self.method == "Click":
            self.expand_page_click()
        elif self.method == "Scroll":
            self.expand_page_scroll()
        elif self.method == "Hybrid":
            self.expand_page_hybrid()
        elif self.method == "Pages":
            self.expand_page_pages()
    def load_data(self):
        self.brand_name=self.data.get("DIRECTORY")
        self.ELEMENT_LOCATOR = self.data.get("ELEMENT_LOCATOR")
        self.POPUP_TEXT = self.data.get("POPUP_TEXT", [])
        self.POPUP_ID = self.data.get("POPUP_ID", [])
        self.POPUP_CLASS = self.data.get("POPUP_CLASS", [])
        self.POPUP_XPATH = self.data.get("POPUP_XPATH", [])
        self.BY_XPATH = self.data.get("BY_XPATH", False)
        self.method = self.data.get("METHOD", "Click")
        self.PRODUCTS_PER_HTML_TAG = self.data.get("PRODUCTS_PER_HTML_TAG", "")
        self.LOCATOR_TYPE = self.BY_XPATH if self.BY_XPATH else By.CSS_SELECTOR
        
    def close_popup(self, wait_time=4):
        if not self.POPUP_TEXT and not self.POPUP_ID and not self.POPUP_CLASS and not self.POPUP_XPATH:
            return
        popup_closed = False
        retries = 0
        max_retries = 6
        while not popup_closed and retries < max_retries:
            if self.POPUP_TEXT:
                try:

                    self.logger.info(f"{self.brand_name}: Trying to close popup with text: {self.POPUP_TEXT}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.XPATH, f'//button[contains(text(), "{self.POPUP_TEXT}")]'))
                    )
                    self.logger.info(close_button)
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", close_button)
                    time.sleep(1)  # Wait for scrolling to complete

                    # Click the close button
                    close_button.click()
                    time.sleep(2)  # Wait for the popup to close
                    popup_closed = True
                    self.logger.info(f"{self.brand_name}: Popup closed successfully")
                    return popup_closed
                except Exception as e:
                    self.logger.exception(f"{self.brand_name}: Popup not found or not clickable: {e}")
                    retries += 1
                    self.logger.exception(f"{self.brand_name}: Attempt {retries}/{max_retries}")
                    time.sleep(2)  # Wait a bit before retrying
            if self.POPUP_ID:
                try:
                    self.logger.info(f"{self.brand_name}: Trying to close popup with ID: {self.POPUP_ID}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.ID, self.POPUP_ID))
                    )
                    self.logger.info(close_button)
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", close_button)
                    time.sleep(1)  # Wait for scrolling to complete

                    # Click the close button
                    close_button.click()
                    time.sleep(2)  # Wait for the popup to close
                    popup_closed = True
                    self.logger.info(f"{self.brand_name}: Popup closed successfully")
                    return popup_closed
                except Exception as e:
                    self.logger.exception(f"{self.brand_name}: Popup not found or not clickable: {e}")
                    retries += 1
                    self.logger.exception(f"{self.brand_name}: Attempt {retries}/{max_retries}")
                    time.sleep(2)  # Wait a bit before retrying
            if self.POPUP_CLASS:
                try:

                    self.logger.info(f"{self.brand_name}: Trying to close popup with class: {self.POPUP_CLASS}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.CLASS_NAME, self.POPUP_CLASS))
                    )
                    self.logger.info(close_button)
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", close_button)
                    time.sleep(1)  # Wait for scrolling to complete

                    # Click the close button
                    close_button.click()
                    time.sleep(2)  # Wait for the popup to close
                    popup_closed = True
                    self.logger.info(f"{self.brand_name}: Popup closed successfully")
                    return popup_closed

                except Exception as e:
                    self.logger.exception(f"{self.brand_name}: Popup not found or not clickable: {e}")
                    retries += 1
                    self.logger.exception(f"{self.brand_name}: Attempt {retries}/{max_retries}")
                    time.sleep(2)  # Wait a bit before retrying
            if self.POPUP_XPATH:
                try:

                    # Handle specific popup structure
                    self.logger.info(f"{self.brand_name}: Trying to close popup with XPATH: {self.POPUP_XPATH}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.XPATH, self.POPUP_XPATH))
                    )
                    self.logger.info(close_button)
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", close_button)
                    time.sleep(1)  # Wait for scrolling to complete

                    # Click the close button
                    close_button.click()
                    time.sleep(2)  # Wait for the popup to close
                    popup_closed = True
                    self.logger.info(f"{self.brand_name}: Popup closed successfully")
                    return popup_closed
                except Exception as e:
                    self.logger.exception(f"{self.brand_name}: Popup not found or not clickable: {e}")
                    retries += 1
                    self.logger.exception(f"{self.brand_name}: Attempt {retries}/{max_retries}")
                    time.sleep(2)  # Wait a bit before retrying
                    # Scroll the button into view



        if not popup_closed:
            self.logger.exception("Failed to close the popup after several retries.")
            return False

    def expand_page_pages(self, wait_time=10):
        page_sources=[]
        print("The pages method is being used")
        self.driver.get(self.url)
        WebDriverWait(self.driver, wait_time).until(
            EC.presence_of_element_located((By.TAG_NAME, "body")))  # Wait for the page to load
        #Necessary Variables
        retries=0
        max_retries=5
        page_number=1

        while True:
            # Closing Popups
            for text, id_, class_, xpath in zip(self.POPUP_TEXT, self.POPUP_ID, self.POPUP_CLASS, self.POPUP_XPATH):
                self.logger.info(f"{self.brand_name} {text},{id_}, {class_}, {xpath}")
                self.close_popup()
            page_name = f"Page_{page_number}"
            final_path = os.path.join(self.output_dir, page_name)
            page_source = self.driver.page_source
            page_sources.append(page_source)
            save_html_file(self.driver.current_url, page_source, final_path)
            try:
                # Scroll to the bottom in order to allow items to load
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                if type(self.ELEMENT_LOCATOR) == list:
                    for locator in self.ELEMENT_LOCATOR:
                        self.logger.info(locator)
                        try:
                            for text, id_, class_, xpath in zip(self.POPUP_TEXT, self.POPUP_ID, self.POPUP_CLASS, self.POPUP_XPATH):
                                self.logger.info(f"{self.brand_name} {text},{id_}, {class_}, {xpath}")
                            load_more_button = WebDriverWait(self.driver, wait_time).until(
                                EC.presence_of_element_located((self.LOCATOR_TYPE, self.ELEMENT_LOCATOR))
                            )
                            next_url = load_more_button.get_attribute('href')
                            self.driver.get(next_url)
                            self.logger.info(f"{self.brand_name} The next page has been opened for {self.brand_name}")
                            time.sleep(wait_time)  # Wait for the page to load
                        except Exception as e:
                            self.logger.exception(e)
                            break
                else:
                    try:
                        load_more_button = WebDriverWait(self.driver, wait_time).until(
                                EC.presence_of_element_located((self.LOCATOR_TYPE, self.ELEMENT_LOCATOR))
                            )
                        next_url = load_more_button.get_attribute('href')
                        self.driver.get(next_url)
                        time.sleep(wait_time)  # Wait for the page to load
                    except Exception as e:
                        self.logger.exception(e)
                        break
                if not load_more_button:
                    raise Exception("No load more button found")
                # Wait for the expand button to be present and clickable
                if not next_url and load_more_button:
                    self.logger.info(f"{self.brand_name} managed to load using:\n {load_more_button}")
                    time.sleep(5)
                    # Scroll the button into view
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
                    time.sleep(5)  # Wait for scrolling to complete
                    # Click the expand button using JavaScript to avoid interception
                    self.driver.execute_script("arguments[0].click();", load_more_button)



                # Wait a bit for the page to load more content
                time.sleep(1)
                retries=0
                page_number+=1

            except Exception as e:
                    self.logger.exception(f"{self.brand_name}: Error occurred:\n{e}")
                    time.sleep(5)
                    retries+=1
                    self.logger.exception(f"{self.brand_name} Attempt {retries}/{max_retries}")
                    #If button not exist 10 times give up
                    if retries>=max_retries:
                        break
        self.driver.close()
        return page_sources

    def expand_page_click(self, wait_time=10):
        page_sources = []
        self.logger.info(f"{self.brand_name} The click method is being used")
        self.driver.get(self.url)
        WebDriverWait(self.driver, wait_time).until(
            EC.presence_of_element_located((By.TAG_NAME, "body")))  # Wait for the page to load
        for text, id_, class_, xpath in zip(self.POPUP_TEXT, self.POPUP_ID, self.POPUP_CLASS, self.POPUP_XPATH):
            self.logger.info(f"{self.brand_name} {text},{id_}, {class_}, {xpath}")
            self.close_popup()


        wait = WebDriverWait(self.driver, wait_time)
        retries = 0
        max_retries = 5
        while True:
            try:
                # If there is no button to click stop
                if type(self.ELEMENT_LOCATOR) == list:
                    for locator in self.ELEMENT_LOCATOR:
                        try:
                            load_more_button = WebDriverWait(self.driver, wait_time).until(
                                EC.element_to_be_clickable((self.LOCATOR_TYPE, locator))
                            )
                            break
                        except:
                            continue
                else:
                    load_more_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((self.LOCATOR_TYPE, self.ELEMENT_LOCATOR))
                    )
                if not load_more_button:
                    raise Exception("No load more button found")
                self.logger.info(f"{self.brand_name} managed to load using:\n {load_more_button}")
                # Wait for the expand button to be present and clickable
                time.sleep(5)
                # Scroll the button into view
                self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
                time.sleep(5)  # Wait for scrolling to complete
                # Click the expand button using JavaScript to avoid interception
                self.driver.execute_script("arguments[0].click();", load_more_button)

                # Wait a bit for the page to load more content
                time.sleep(wait_time)
                self.logger.info(f"{self.brand_name} Retries have been reset to 0")
                retries = 0

            except Exception as e:
                self.logger.exception(f"{self.brand_name}: Error occurred:\n{e}")
                time.sleep(2)
                retries += 1
                self.logger.exception(f"{self.brand_name} Attempt {retries}/{max_retries}")
                # If button not exist 10 times give up
                if retries >= max_retries:
                    break

        page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
        save_html_file(self.url, page_source, self.output_dir)
        page_sources.append(page_source)
        self.driver.close()
        return page_sources
    def expand_page_hybrid(self,wait_time=6, scroll_pause_time=6, initial_scroll_back_amount=300):
        page_sources=[]
        max_retries = 10
        self.logger.info(f"{self.brand_name} The hybrid method is being used")
        self.driver.get(self.url)
        WebDriverWait(self.driver, wait_time).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # Handle popups
        for text, id_, class_, xpath in zip(self.POPUP_TEXT or [], self.POPUP_ID or [], self.POPUP_CLASS or [], self.POPUP_XPATH or []):
            self.logger.info(f"{self.brand_name} {text},{id_}, {class_}, {xpath}")
            self.close_popup()


        last_height = self.driver.execute_script("return document.body.scrollHeight")
        no_changes_count = 0
        scroll_back_amount = initial_scroll_back_amount

        while True:

            # Try to click the "load more" button if it exists
            try:
                if type(self.ELEMENT_LOCATOR) == list:
                    for locator in self.ELEMENT_LOCATOR:
                        try:
                            load_more_button = WebDriverWait(self.driver, wait_time).until(
                                EC.element_to_be_clickable((self.LOCATOR_TYPE, locator))
                            )
                            break
                        except:
                            continue
                else:
                    load_more_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((self.LOCATOR_TYPE, self.ELEMENT_LOCATOR))
                    )
                if not load_more_button:
                    raise Exception("No load more button found")
                self.logger.info(f"{self.brand_name} managed to load using: {load_more_button}")
                self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
                time.sleep(2)
                self.driver.execute_script("arguments[0].click();", load_more_button)
                time.sleep(wait_time)  # Wait for new content to load
                self.logger.info(f"{self.brand_name}: Clicked 'load more' button")
                no_changes_count = 0  # Reset no changes count
            except Exception as e:
                self.logger.exception(f"{self.brand_name}: No 'load more' button found or not clickable: {e}")

                # If button not found, scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_pause_time)
                self.driver.execute_script("window.scrollBy(0, -arguments[0]);", scroll_back_amount)
                time.sleep(scroll_pause_time)

                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    no_changes_count += 1
                    scroll_back_amount += 200
                    self.logger.exception(f"{self.brand_name}: No change in height. Attempt {no_changes_count}/{max_retries}")
                    if no_changes_count >= max_retries:
                        self.logger.exception(f"{self.brand_name} Reached the bottom of the page or no more content loading")
                        break
                else:
                    scroll_back_amount=initial_scroll_back_amount
                    no_changes_count = 0  # Reset count if height changed
                    self.logger.exception(f"{self.brand_name} Successfully scrolled to bottom loading new items.")

                last_height = new_height


        page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
        save_html_file(self.url, page_source, self.output_dir)
        page_sources.append(page_source)
        self.driver.close()
        return page_sources

    def expand_page_scroll(self, initial_scroll_back_amount=300, wait_time=10, scroll_pause_time=10):
        max_retries = 10
        page_sources=[]
        self.logger.info(f"{self.brand_name} The scroll method is being used")
        self.driver.get(self.url)
        WebDriverWait(self.driver, wait_time).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        for text, id_, class_, xpath in zip(self.POPUP_TEXT, self.POPUP_ID, self.POPUP_CLASS, self.POPUP_XPATH):
            self.logger.info(f"{self.brand_name} {text},{id_}, {class_}, {xpath}")
            self.close_popup()


        last_height = self.driver.execute_script("return document.body.scrollHeight")
        retry_count = 0
        scroll_back_amount = initial_scroll_back_amount
        while True:
            # Scroll to the bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause_time)

            # Scroll back up by the set amount
            self.driver.execute_script("window.scrollBy(0, -arguments[0]);", scroll_back_amount)
            time.sleep(2)

            # Scroll to the bottom again
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Check if the page height has changed
            new_height = self.driver.execute_script("return document.body.scrollHeight")

            if new_height == last_height:
                retry_count += 1
                scroll_back_amount += 200  # Increase scroll back amount to ensure it's not missing content
                self.logger.info(f"{self.brand_name}: No change in height. Attempt {retry_count}/{max_retries}")
                if retry_count >= max_retries:
                    break
            else:
                scroll_back_amount = initial_scroll_back_amount
                retry_count = 0  # Reset retry count if new content is loaded
                self.logger.info(f"{self.brand_name}: Successfully Scrolled")
            last_height = new_height

        page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
        save_html_file(self.url, page_source, self.output_dir)
        page_sources.append(page_source)
        self.driver.close()
        return page_sources

def read_file_to_list(file_path):
    with open(file_path, 'r') as file:
        lines = file.read().splitlines()
    return lines


def save_html_file(url, html_content, base_directory):
    # Extract the path from the URL
    path = url.split("//")[-1]  # Remove protocol and get the path
    path = re.sub(r'[<>:"\\|?*]', '_', path)
    path_parts = path.split("/")
    # Construct the full directory path
    full_dir_path = os.path.join(base_directory, *path_parts[:-1])

    # Create the directory if it doesn't exist
    os.makedirs(full_dir_path, exist_ok=True)

    # Construct the file path
    filename = path_parts[-1] + ".html" if path_parts[-1] else path_parts[-2] +".html"
    filepath = os.path.join(full_dir_path, filename)

    # Write the HTML content to the file
    with open(filepath, "w", encoding="utf-8") as file:
        file.write(html_content)

    print(f"Saved: {filepath}")

current_directory = os.getcwd()
google_directory = r"G:\.shortcut-targets-by-id\12IWJ5XcazqdC1N-wiTEbQ5cBK0bei0kk\msrp_archive_raw"
directory_to_be_run="No_EURO_yet"
print(current_directory)


def count_products(html_content, tag):
    if not tag:
        return 0
    soup = BeautifulSoup(html_content, 'html.parser')
    products = soup.find_all(class_=tag)
    print(f"Number of products found: {len(products)}")
    return len(products)


def save_to_csv(brand_name, url_product_counts):
    brand_name=brand_name.lower()
    filename = os.path.join(current_directory,"Product_Counts", f"{brand_name}_product_counts.csv")
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['URL', 'Product Count'])
        for url, count in url_product_counts:
            writer.writerow([url, count])


def read_file_to_list(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file]


SETTINGS_URL = "https://raw.githubusercontent.com/samuelshlyam/HTML_Gather_Settings/dev/settings.json"


def fetch_settings():
    try:
        response = requests.get(SETTINGS_URL)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching settings: {e}")
        return None
@app.post("/fetch_html")
async def brand_batch_endpoint(brand_id: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(process_brand_batch, brand_id)
    return {"message": "Notification sent in the background"}
def process_brand_batch(brand_id):
    jsonData = fetch_settings()
    data = jsonData.get(brand_id)
    URL_LIST = data.get('URL_LIST')
    brand_name= data.get('DIRECTORY')
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = os.path.join(current_directory, 'Outputs', brand_name,time_stamp)
    print(f"Number of URLs: {len(URL_LIST)}")

    os.makedirs(output_dir, exist_ok=True)

    expanders = [PageExpander.remote(brand_id,url,output_dir) for url in URL_LIST]
    results = ray.get([expander.start.remote() for expander in expanders])
    print(results)


# start = datetime.datetime.now()
#
# # Fetch settings from the GitHub URL
#
#
# if jsonData is None:
#     print("Failed to fetch settings. Exiting.")
# else:
#     # Get the brand ID from user input
#     brand_id = input("Enter the brand ID to process: ")
#
#     if brand_id not in jsonData:
#         print(f"Brand ID '{brand_id}' not found in settings.")
#     else:
#         process_brand(expander, brand_id, jsonData[brand_id])
#
# end = datetime.datetime.now()
# print(f"\nProcessing completed.\nTime taken: {end - start}")

if __name__ == "__main__":
    uvicorn.run("main:app", port=8000, log_level="info")



