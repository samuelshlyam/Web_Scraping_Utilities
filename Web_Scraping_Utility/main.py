from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import json
import time
import re


class PageExpander:
    def __init__(self, driver):
        self.driver = driver

    def close_popup(self, popup_text=None, popup_id=None, popup_class=None, wait_time=10):
        if not popup_text and not popup_id and not popup_class:
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
                if popup_id:
                    print(f"Trying to close popup with ID: {popup_id}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.ID, popup_id))
                    )
                if popup_class:
                    print(f"Trying to close popup with class: {popup_class}")
                    close_button = WebDriverWait(self.driver, wait_time).until(
                        EC.element_to_be_clickable((By.CLASS_NAME, popup_class))
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

    def expand_page_click(self, urls, element_locator, locator_type=By.CSS_SELECTOR, wait_time=5, popup_id=None, popup_text=None,popup_class=None):
        page_sources=[]
        for url in urls:
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "body")))  # Wait for the page to load
            for text, id_, class_ in zip(popup_text, popup_id, popup_class):
                print(text, id_, class_)
                self.close_popup(popup_text=text, popup_id=id_, popup_class=class_)

            wait = WebDriverWait(self.driver, wait_time)
            count=0
            while True:
                try:
                    #If there is no button to click stop
                    if not wait.until(EC.presence_of_element_located((locator_type, element_locator))):
                        break
                    # Wait for the expand button to be present and clickable
                    element = wait.until(EC.element_to_be_clickable((locator_type, element_locator)))
                    print(element)
                    # Scroll the button into view
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                    time.sleep(1)  # Wait for scrolling to complete

                    # Click the expand button using JavaScript to avoid interception
                    self.driver.execute_script("arguments[0].click();", element)
                    # Wait a bit for the page to load more content
                    time.sleep(2)


                except Exception as e:
                        print(f"Error occurred: {e}")
                        time.sleep(2)
                        count+=1
                        #If button not exist 10 times give up
                        if count>=5:
                            break

            page_source = self.driver.page_source
            page_sources.append(page_source)
        return page_sources

    def expand_page_scroll(self, urls, initial_scroll_back_amount=500, wait_time=10, scroll_pause_time=2, max_time=6000, popup_id=None, popup_text=None, popup_class=None):
        page_sources = []
        for url in urls:
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            for text, id_, class_ in zip(popup_text, popup_id, popup_class):
                print(text, id_, class_)
                self.close_popup(popup_text=text, popup_id=id_, popup_class=class_)

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
                    if retry_count >= 5:
                        break
                else:
                    retry_count = 0  # Reset retry count if new content is loaded

                last_height = new_height

            page_source = self.driver.page_source
            page_sources.append(page_source)
        return page_sources

def read_file_to_list(file_path):
    with open(file_path, 'r') as file:
        lines = file.read().splitlines()
    return lines
# Example usage:
if __name__ == "__main__":
    driver = webdriver.Chrome()
    expander = PageExpander(driver)

    with open("settings.json", "r") as file:
        jsonData = json.load(file)

    data=jsonData["Kenzo"]
    method=data['METHOD']
    URL_LIST = read_file_to_list(data['DIRECTORY'])
    ELEMENT_LOCATOR = data["ELEMENT_LOCATOR"]
    POPUP_TEXT=data["POPUP_TEXT"]
    POPUP_ID = data["POPUP_ID"]
    POPUP_CLASS=data["POPUP_CLASS"]
    print(method,ELEMENT_LOCATOR,POPUP_TEXT,POPUP_ID,POPUP_CLASS)
    print(URL_LIST)

    #Only define if absolutely necessary (Alexander McQueen : LOCATOR_TYPE=By.XPATH
    # most need By.CSS_SELECTOR)
    LOCATOR_TYPE=By.CSS_SELECTOR

    if method=="Click":
        expanded_page_sources = expander.expand_page_click(URL_LIST, ELEMENT_LOCATOR,locator_type=LOCATOR_TYPE, popup_text=POPUP_TEXT, popup_id=POPUP_ID, popup_class=POPUP_CLASS)
    elif method=="Scroll":
        expanded_page_sources = expander.expand_page_scroll(URL_LIST,popup_text=POPUP_TEXT, popup_id=POPUP_ID, popup_class=POPUP_CLASS)
    print(expanded_page_sources)
#Issues Balmain and Birkenstock popups don't close