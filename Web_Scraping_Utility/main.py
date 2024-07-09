from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import threading
import queue
import time
from bs4 import BeautifulSoup
import json
import time
import re
import os
import datetime
import logging

class PageExpander:

    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--auto-open-devtools-for-tabs")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
        self.driver = webdriver.Chrome( options=options)
        self.brand_name=''

    def close_popup(self, popup_text=None, popup_id=None, popup_class=None, popup_xpath=None, wait_time=3):
        if not popup_text and not popup_id and not popup_class and not popup_xpath:
            return
        print("I made it in here")
        popup_closed = False
        retries = 0
        max_retries = 12
        count=0
        while not popup_closed and retries < max_retries:
            try:
                count+=1
                if popup_text and count%4==0:
                    print(f"{self.brand_name}: Trying to close popup with text: {popup_text}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.XPATH, f'//button[contains(text(), "{popup_text}")]'))
                    )
                elif popup_id and count%4==1:
                    print(f"{self.brand_name}: Trying to close popup with ID: {popup_id}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.ID, popup_id))
                    )
                elif popup_class and count%4==2:
                    print(f"{self.brand_name}: Trying to close popup with class: {popup_class}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.CLASS_NAME, popup_class))
                    )
                    print(close_button)
                elif popup_xpath and count%4==3:
                    # Handle specific popup structure
                    print(f"{self.brand_name}: Trying to close popup with XPATH: {popup_xpath}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.XPATH, popup_xpath))
                    )
                    print(close_button)
                # Scroll the button into view
                self.driver.execute_script("arguments[0].scrollIntoView(true);", close_button)
                time.sleep(1)  # Wait for scrolling to complete

                # Click the close button
                close_button.click()
                time.sleep(2)  # Wait for the popup to close
                popup_closed = True
                print(f"{self.brand_name}: Popup closed successfully")
                return popup_closed
            except Exception as e:
                print(f"{self.brand_name}: Popup not found or not clickable: {e}")
                retries += 1
                print(f"{self.brand_name}: Attempt {retries}/{max_retries}")
                time.sleep(2)  # Wait a bit before retrying

        if not popup_closed:
            print("Failed to close the popup after several retries.")

    def expand_page_pages(self, urls, element_locator, locator_type=By.CSS_SELECTOR, wait_time=10, popup_id=None, popup_text=None,popup_class=None,popup_xpath=None):
        time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        new_path = os.path.join(current_directory, "Outputs", self.brand_name, time_stamp)
        logging_file_path = os.path.join(new_path, f"{self.brand_name}.log")
        os.makedirs(new_path)
        print(logging_file_path)
        logging.basicConfig(filename=logging_file_path, level=logging.DEBUG)
        logger = logging.getLogger()
        page_sources=[]
        print("The pages method is being used")
        for index,url in enumerate(urls):
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "body")))  # Wait for the page to load
            #Necessary Variables
            retries=0
            max_retries=5
            page_number=1

            while True:
                # Closing Popups
                for text, id_, class_, xpath in zip(popup_text, popup_id, popup_class, popup_xpath):
                    logger.info(text, id_, class_, xpath)
                    self.close_popup(popup_text=text, popup_id=id_, popup_class=class_, popup_xpath=xpath)
                page_name = f"Page_{page_number}"
                final_path = os.path.join(new_path, page_name)
                page_source = self.driver.page_source
                page_sources.append(page_source)
                save_html_file(self.driver.current_url, page_source, final_path)
                try:
                    # Scroll to the bottom in order to allow items to load
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    if type(element_locator) == list:
                        for locator in element_locator:
                            logger.info(locator)
                            try:
                                for text, id_, class_, xpath in zip(popup_text, popup_id, popup_class, popup_xpath):
                                    logger.info(text, id_, class_, xpath)
                                load_more_button = WebDriverWait(self.driver, wait_time).until(
                                    EC.presence_of_element_located((locator_type, element_locator))
                                )
                                next_url = load_more_button.get_attribute('href')
                                self.driver.get(next_url)
                                logger.info(f"The next page has been opened for {self.brand_name}")
                                time.sleep(2)  # Wait for the page to load
                            except Exception as e:
                                logger.info(e)
                                break
                    else:
                        try:
                            load_more_button = WebDriverWait(self.driver, wait_time).until(
                                    EC.presence_of_element_located((locator_type, element_locator))
                                )
                            next_url = load_more_button.get_attribute('href')
                            self.driver.get(next_url)
                            time.sleep(2)  # Wait for the page to load
                        except Exception as e:
                            logger.info(e)
                            break
                    if not load_more_button:
                        raise Exception("No load more button found")
                    # Wait for the expand button to be present and clickable
                    if not next_url and load_more_button:
                        logger.info(f"{self.brand_name} managed to load using:\n {load_more_button}")
                        time.sleep(1)
                        # Scroll the button into view
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
                        time.sleep(1)  # Wait for scrolling to complete
                        # Click the expand button using JavaScript to avoid interception
                        self.driver.execute_script("arguments[0].click();", load_more_button)



                    # Wait a bit for the page to load more content
                    time.sleep(1)
                    retries=0
                    page_number+=1

                except Exception as e:
                        logger.info(f"{self.brand_name}: Error occurred:\n{e}")
                        time.sleep(2)
                        retries+=1
                        logger.info(f"Attempt {retries}/{max_retries}")
                        #If button not exist 10 times give up
                        if retries>=max_retries:
                            break
        self.driver.close()
        return page_sources

    def expand_page_click(self, urls, element_locator, locator_type=By.CSS_SELECTOR, wait_time=10,
                          popup_id=None, popup_text=None, popup_class=None, popup_xpath=None):
        time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        new_path = os.path.join(current_directory, "Outputs", self.brand_name, time_stamp)
        logging_file_path = os.path.join(new_path, f"{self.brand_name}.log")
        os.makedirs(new_path)
        print(logging_file_path)
        logging.basicConfig(filename=logging_file_path, level=logging.DEBUG)
        logger = logging.getLogger()
        page_sources = []
        logger.info("The click method is being used")
        for url in urls:
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "body")))  # Wait for the page to load
            for text, id_, class_, xpath in zip(popup_text, popup_id, popup_class, popup_xpath):
                logger.info(text, id_, class_, xpath)
                self.close_popup(popup_text=text, popup_id=id_, popup_class=class_, popup_xpath=xpath)


            wait = WebDriverWait(self.driver, wait_time)
            retries = 0
            max_retries = 5
            while True:
                try:
                    # If there is no button to click stop
                    if type(element_locator) == list:
                        for locator in element_locator:
                            try:
                                load_more_button = WebDriverWait(self.driver, wait_time).until(
                                    EC.element_to_be_clickable((locator_type, locator))
                                )
                                break
                            except:
                                continue
                    else:
                        load_more_button = WebDriverWait(self.driver, wait_time).until(
                            EC.element_to_be_clickable((locator_type, element_locator))
                        )
                    if not load_more_button:
                        raise Exception("No load more button found")
                    logger.info(f"{self.brand_name} managed to load using:\n {load_more_button}")
                    # Wait for the expand button to be present and clickable
                    time.sleep(1)
                    # Scroll the button into view
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
                    time.sleep(1)  # Wait for scrolling to complete
                    # Click the expand button using JavaScript to avoid interception
                    self.driver.execute_script("arguments[0].click();", load_more_button)

                    # Wait a bit for the page to load more content
                    time.sleep(1)
                    retries = 0

                except Exception as e:
                    logger.info(f"{self.brand_name}: Error occurred:\n{e}")
                    time.sleep(2)
                    retries += 1
                    logger.info(f"{self.brand_name} Attempt {retries}/{max_retries}")
                    # If button not exist 10 times give up
                    if retries >= max_retries:
                        break

            page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
            save_html_file(url, page_source, new_path)
            page_sources.append(page_source)
        self.driver.close()
        return page_sources
    def expand_page_hybrid(self, urls, element_locator, locator_type=By.CSS_SELECTOR,
                       wait_time=10, scroll_pause_time=5,
                       popup_text=None, popup_id=None, popup_class=None, popup_xpath=None,initial_scroll_back_amount=300):
        time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        new_path = os.path.join(current_directory, "Outputs", self.brand_name, time_stamp)
        logging_file_path = os.path.join(new_path, f"{self.brand_name}.log")
        os.makedirs(new_path)
        print(logging_file_path)
        logging.basicConfig(filename=logging_file_path, level=logging.DEBUG)
        logger = logging.getLogger()
        page_sources=[]
        max_retries = 10
        logger.info("The hybrid method is being used")
        for url in urls:
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Handle popups
            for text, id_, class_, xpath in zip(popup_text or [], popup_id or [], popup_class or [], popup_xpath or []):
                logger.info(text, id_, class_, xpath)
                self.close_popup(popup_text=text, popup_id=id_, popup_class=class_, popup_xpath=xpath)


            last_height = self.driver.execute_script("return document.body.scrollHeight")
            no_changes_count = 0
            scroll_back_amount = initial_scroll_back_amount

            while True:

                # Try to click the "load more" button if it exists
                try:
                    if type(element_locator) == list:
                        for locator in element_locator:
                            try:
                                load_more_button = WebDriverWait(self.driver, wait_time).until(
                                    EC.element_to_be_clickable((locator_type, locator))
                                )
                                break
                            except:
                                continue
                    else:
                        load_more_button = WebDriverWait(self.driver, wait_time).until(
                            EC.element_to_be_clickable((locator_type, element_locator))
                        )
                    if not load_more_button:
                        raise Exception("No load more button found")
                    logger.info(f"{self.brand_name} managed to load using: {load_more_button}")
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
                    time.sleep(1)
                    self.driver.execute_script("arguments[0].click();", load_more_button)
                    time.sleep(wait_time)  # Wait for new content to load
                    logger.info(f"{self.brand_name}: Clicked 'load more' button")
                    no_changes_count = 0  # Reset no changes count
                except Exception as e:
                    logger.info(f"{self.brand_name}: No 'load more' button found or not clickable: {e}")
                    
                    # If button not found, scroll to bottom
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(scroll_pause_time)
                    self.driver.execute_script("window.scrollBy(0, -arguments[0]);", scroll_back_amount)
                    time.sleep(scroll_pause_time)

                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        no_changes_count += 1
                        scroll_back_amount += 200
                        logger.info(f"{self.brand_name}: No change in height. Attempt {no_changes_count}/{max_retries}")
                        if no_changes_count >= max_retries:
                            logger.info(f"Reached the bottom of the page or no more content loading\nFor {self.brand_name}")
                            break
                    else:
                        scroll_back_amount=initial_scroll_back_amount
                        no_changes_count = 0  # Reset count if height changed
                        logger.info(f"Successfully scrolled to bottom loading new items.\nFor {self.brand_name}")
                    
                    last_height = new_height


            page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
            save_html_file(url, page_source, new_path)
            page_sources.append(page_source)
        self.driver.close()
        return page_sources
    def expand_page_scroll(self, urls, initial_scroll_back_amount=300, wait_time=10, scroll_pause_time=5, popup_id=None, popup_text=None, popup_class=None,popup_xpath=None):
        time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        new_path = os.path.join(current_directory, "Outputs", self.brand_name, time_stamp)
        logging_file_path=os.path.join(new_path, f"{self.brand_name}.log")
        os.makedirs(new_path)
        print(logging_file_path)
        logging.basicConfig(filename=logging_file_path,level=logging.DEBUG)
        logger = logging.getLogger()
        max_retries = 10
        page_sources=[]
        logger.info("The scroll method is being used")
        for url in urls:
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            for text, id_, class_, xpath in zip(popup_text, popup_id, popup_class, popup_xpath):
                logger.info(text, id_, class_, xpath)
                self.close_popup(popup_text=text, popup_id=id_, popup_class=class_, popup_xpath=xpath)


            last_height = self.driver.execute_script("return document.body.scrollHeight")
            retry_count = 0
            scroll_back_amount = initial_scroll_back_amount
            while True:
                # Scroll to the bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_pause_time)

                # Scroll back up by the set amount
                self.driver.execute_script("window.scrollBy(0, -arguments[0]);", scroll_back_amount)
                time.sleep(scroll_pause_time)

                # Scroll to the bottom again
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_pause_time)

                # Check if the page height has changed
                new_height = self.driver.execute_script("return document.body.scrollHeight")

                if new_height == last_height:
                    retry_count += 1
                    scroll_back_amount += 200  # Increase scroll back amount to ensure it's not missing content
                    logger.info(f"{self.brand_name}: No change in height. Attempt {retry_count}/{max_retries}")
                    if retry_count >= max_retries:
                        break
                else:
                    scroll_back_amount = initial_scroll_back_amount
                    retry_count = 0  # Reset retry count if new content is loaded
                    logger.info(f"{self.brand_name}: Successfully Scrolled")
                last_height = new_height

            page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
            save_html_file(url, page_source, new_path)
            page_sources.append(page_source)
        self.driver.close()
        return page_sources

def read_file_to_list(file_path):
    with open(file_path, 'r') as file:
        lines = file.read().splitlines()
    return lines


def save_html_file(url, html_content, base_directory):
    # Extract the path from the URL
    path = url.split("//")[-1].split("/", 1)[-1]  # Remove protocol and get the path
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
directory_to_be_run="Brand_URLS_Testing"
print(current_directory)

import threading
import queue
import time
import csv
from bs4 import BeautifulSoup

def count_products(html_content, tag):
    if not tag:
        return 0
    soup = BeautifulSoup(html_content, 'html.parser')
    products = soup.find_all(class_=tag)
    print(f"Number of products found: {len(products)}")
    return len(products)


def save_to_csv(brand_name, url_product_counts):
    filename = os.path.join(current_directory,"Product_Counts", f"{brand_name}_product_counts.csv")
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['URL', 'Product Count'])
        for url, count in url_product_counts:
            writer.writerow([url, count])

def process_brand(expander, brand_name, data, current_directory):
    print(f"\nProcessing brand: {brand_name}")
    
    directory = os.path.join(current_directory, directory_to_be_run, f"{data['DIRECTORY']}.txt")
    try:
        URL_LIST = read_file_to_list(directory)
    except FileNotFoundError:
        print(f"URL list file not found for {brand_name}. Skipping.")
        return

    ELEMENT_LOCATOR = data.get("ELEMENT_LOCATOR")
    POPUP_TEXT = data.get("POPUP_TEXT", [])
    POPUP_ID = data.get("POPUP_ID", [])
    POPUP_CLASS = data.get("POPUP_CLASS", [])
    POPUP_XPATH = data.get("POPUP_XPATH", [])
    BY_XPATH = data.get("BY_XPATH", False)
    method = data.get("METHOD", "Click")
    PRODUCTS_PER_HTML_TAG = data.get("PRODUCTS_PER_HTML_TAG", "")
    brand_name=data.get("DIRECTORY","")
    expander.brand_name=brand_name
    print(f"Method: {method}, Element Locator: {ELEMENT_LOCATOR}")
    print(f"Popup Text: {POPUP_TEXT}, Popup ID: {POPUP_ID}, Popup Class: {POPUP_CLASS}, Popup XPath: {POPUP_XPATH}")
    print(f"Products Per HTML Tag: {PRODUCTS_PER_HTML_TAG}")
    print(f"URLs: {URL_LIST}")

    LOCATOR_TYPE = By.XPATH if BY_XPATH else By.CSS_SELECTOR

    url_product_counts = []

    try:
        if method == "Click":
            html_contents = expander.expand_page_click(URL_LIST, ELEMENT_LOCATOR, locator_type=LOCATOR_TYPE,
                                       popup_text=POPUP_TEXT, popup_id=POPUP_ID, popup_class=POPUP_CLASS, popup_xpath=POPUP_XPATH)
        elif method == "Scroll":
            html_contents = expander.expand_page_scroll(URL_LIST, popup_text=POPUP_TEXT, popup_id=POPUP_ID,
                                        popup_class=POPUP_CLASS, popup_xpath=POPUP_XPATH)
        elif method == "Hybrid":
            html_contents = expander.expand_page_hybrid(URL_LIST, ELEMENT_LOCATOR,
                                                        locator_type=LOCATOR_TYPE,
                                                        popup_text=POPUP_TEXT, popup_id=POPUP_ID,
                                                        popup_class=POPUP_CLASS, popup_xpath=POPUP_XPATH)
        elif method == "Pages":
            html_contents = expander.expand_page_pages(URL_LIST, ELEMENT_LOCATOR,
                                                        locator_type=LOCATOR_TYPE,
                                                        popup_text=POPUP_TEXT, popup_id=POPUP_ID,
                                                        popup_class=POPUP_CLASS, popup_xpath=POPUP_XPATH)
        else:
            print(f"Unknown method '{method}' for {brand_name}. Skipping.")
            return

        for url, html_content in zip(URL_LIST, html_contents):
            product_count = count_products(html_content, PRODUCTS_PER_HTML_TAG)
            url_product_counts.append((url, product_count))
            print(f"URL: {url}, Product Count: {product_count}")

        save_to_csv(brand_name, url_product_counts)

    except Exception as e:
        print(f"An error occurred while processing {brand_name}: {str(e)}")

def worker(task_queue, expander, current_directory):
    while True:
        try:
            brand_name, data = task_queue.get(block=False)
            process_brand(expander, brand_name, data, current_directory)
            task_queue.task_done()
        except queue.Empty:
            break


ABS_MAX_THREADS=10
MAX_THREADS=len(os.listdir(os.path.join(current_directory,directory_to_be_run)))

if __name__ == "__main__":
    start=datetime.datetime.now()
    with open("settings.json", "r") as file:
        jsonData = json.load(file)

    task_queue = queue.Queue()
    for brand_name, data in jsonData.items():
        task_queue.put((brand_name, data))

    num_threads = min(len(jsonData), MAX_THREADS,ABS_MAX_THREADS)  # Adjust the number of threads as needed
    threads = []

    for _ in range(num_threads):
        expander = PageExpander()  # Create a new PageExpander instance for each thread
        t = threading.Thread(target=worker, args=(task_queue, expander, current_directory))
        t.start()
        threads.append(t)

    # Wait for all tasks to be completed
    task_queue.join()

    # Stop workers
    for _ in range(num_threads):
        task_queue.put(None)
    for t in threads:
        t.join()
    end=datetime.datetime.now()
    print(f"\nAll brands processed.\n Taking: {end-start}")

#Issues Birkenstock popups don't close
#Issues Versace Find More button can't be found/clicked
#Prada not all products are grabbed consistently
#Versace not all products are grabbed consistently
#Palm Angels has networking issues
#Off White has networking issues
#Recheck that Jimmy Choo is working properly
#(taking a long time to load each time may not  get all items)
#Canada Goose Pages do not open
#Isabel Marant needs to be visible or doesnt scroll correctly
#Valentino has some annoying issue idk why
#Loro Piana needs to be open and visible in order to work
#Givenchy acts weirdly
#Saint Laurent needs to be open
#Write Bally Parser