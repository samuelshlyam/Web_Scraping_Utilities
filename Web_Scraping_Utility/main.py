from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

import json
import time
import re
import os


class PageExpander:

    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--auto-open-devtools-for-tabs")
        self.driver = webdriver.Chrome( options=options)

    def close_popup(self, popup_text=None, popup_id=None, popup_class=None, popup_xpath=None, wait_time=10):
        if not popup_text and not popup_id and not popup_class and not popup_xpath:
            return
        print("I made it in here")
        popup_closed = False
        retries = 0
        max_retries = 5

        while not popup_closed and retries < max_retries:
            try:
                if popup_text:
                    print(f"Trying to close popup with text: {popup_text}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.XPATH, f'//button[contains(text(), "{popup_text}")]'))
                    )
                elif popup_id:
                    print(f"Trying to close popup with ID: {popup_id}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.ID, popup_id))
                    )
                elif popup_class:
                    print(f"Trying to close popup with class: {popup_class}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.CLASS_NAME, popup_class))
                    )
                    print(close_button)
                else:
                    # Handle specific popup structure
                    print(f"Trying to close popup with XPATH: {popup_xpath}")
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
                print("Popup closed successfully")
            except Exception as e:
                print(f"Popup not found or not clickable: {e}")
                retries += 1
                time.sleep(2)  # Wait a bit before retrying

        if not popup_closed:
            print("Failed to close the popup after several retries.")

    def expand_page_click(self, urls, element_locator, brand_name, locator_type=By.CSS_SELECTOR, wait_time=3, popup_id=None, popup_text=None,popup_class=None,popup_xpath=None):
        current_directory
        new_path = current_directory + "\\Outputs\\" + brand_name
        for url in urls:
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "body")))  # Wait for the page to load
            for text, id_, class_, xpath in zip(popup_text, popup_id, popup_class, popup_xpath):
                print(text, id_, class_, xpath)
                self.close_popup(popup_text=text, popup_id=id_, popup_class=class_, popup_xpath=xpath)

            wait = WebDriverWait(self.driver, wait_time)
            count=0
            while True:
                try:
                    # Scroll to the bottom in order to allow items to load
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    #If there is no button to click stop
                    if not wait.until(EC.presence_of_element_located((locator_type, element_locator))):
                        break
                    # Wait for the expand button to be present and clickable
                    element = wait.until(EC.element_to_be_clickable((locator_type, element_locator)))
                    print(element)
                    time.sleep(1)
                    # Scroll the button into view
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                    time.sleep(1)  # Wait for scrolling to complete
                    # Click the expand button using JavaScript to avoid interception
                    self.driver.execute_script("arguments[0].click();", element)

                    # Wait a bit for the page to load more content
                    time.sleep(1)
                    count=0

                except Exception as e:
                        print(f"Error occurred: {e}")
                        time.sleep(2)
                        count+=1
                        #If button not exist 10 times give up
                        if count>=5:
                            break

            page_source = self.driver.page_source
            save_html_file(url, page_source, new_path)


    def expand_page_scroll(self, urls, brand_name, initial_scroll_back_amount=500, wait_time=3, scroll_pause_time=2, max_time=6000, popup_id=None, popup_text=None, popup_class=None,popup_xpath=None):
        current_directory
        new_path = current_directory + "\\Outputs\\" + brand_name
        for url in urls:
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            for text, id_, class_, xpath in zip(popup_text, popup_id, popup_class, popup_xpath):
                print(text, id_, class_, xpath)
                self.close_popup(popup_text=text, popup_id=id_, popup_class=class_, popup_xpath=xpath)

            last_height = self.driver.execute_script("return document.body.scrollHeight")
            retry_count = 0
            scroll_back_amount = initial_scroll_back_amount
            start_time = time.time()

            while True:
                # Check if the maximum time has been exceeded
                if time.time() - start_time > max_time:
                    print("Maximum time limit reached.")
                    break

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
                    print("Couldn't Scroll")
                    if retry_count >= 5:
                        break
                else:
                    retry_count = 0  # Reset retry count if new content is loaded
                    print("Successfully Scrolled")
                last_height = new_height

            page_source = self.driver.page_source
            save_html_file(url, page_source, new_path)

def read_file_to_list(file_path):
    with open(file_path, 'r') as file:
        lines = file.read().splitlines()
    return lines


def save_html_file(url, html_content, base_directory):
    # Extract the path from the URL
    path = url.split("//")[-1].split("/", 1)[-1]  # Remove protocol and get the path
    path_parts = path.split("/")

    # Construct the full directory path
    full_dir_path = os.path.join(base_directory, *path_parts[:-1])

    # Create the directory if it doesn't exist
    os.makedirs(full_dir_path, exist_ok=True)

    # Construct the file path
    filename = path_parts[-1] + ".html"
    filepath = os.path.join(full_dir_path, filename)

    # Write the HTML content to the file
    with open(filepath, "w", encoding="utf-8") as file:
        file.write(html_content)

    print(f"Saved: {filepath}")

current_directory = os.getcwd()
print(current_directory)
# Example usage:
if __name__ == "__main__":
    expander = PageExpander()

    with open("settings.json", "r") as file:
        jsonData = json.load(file)
    brand_name="Versace"
    data=jsonData[brand_name]
    brand_name = data['DIRECTORY']
    method=data['METHOD']
    directory=current_directory+r"\Brand_URLS\\"+brand_name
    URL_LIST = read_file_to_list(directory)
    ELEMENT_LOCATOR = data["ELEMENT_LOCATOR"]
    POPUP_TEXT=data["POPUP_TEXT"]
    POPUP_ID = data["POPUP_ID"]
    POPUP_CLASS=data["POPUP_CLASS"]
    POPUP_XPATH=data["POPUP_XPATH"]
    BY_XPATH=data["BY_XPATH"]
    print(method,ELEMENT_LOCATOR,POPUP_TEXT,POPUP_ID,POPUP_CLASS,POPUP_XPATH)
    print(URL_LIST)

    if BY_XPATH:
        LOCATOR_TYPE=By.XPATH
    else:
        LOCATOR_TYPE=By.CSS_SELECTOR


    if method=="Click":
        expander.expand_page_click(URL_LIST, ELEMENT_LOCATOR,brand_name,locator_type=LOCATOR_TYPE, popup_text=POPUP_TEXT, popup_id=POPUP_ID, popup_class=POPUP_CLASS,popup_xpath=POPUP_XPATH)
    elif method=="Scroll":
        expander.expand_page_scroll(URL_LIST,brand_name,popup_text=POPUP_TEXT, popup_id=POPUP_ID, popup_class=POPUP_CLASS, popup_xpath=POPUP_XPATH)


#Issues Birkenstock popups don't close
#Issues Versace Find More button can't be found/clicked