import os
import csv
from bs4 import BeautifulSoup

current_directory = os.getcwd()
print(current_directory)

def count_products(html_content, tag):
    if not tag:
        return 0
    soup = BeautifulSoup(html_content, 'html.parser')
    products = soup.find_all(tag)
    print(f"Number of products found: {len(products)}")
    return len(products)

def save_to_csv(brand_name, url_product_counts):
    filename = os.path.join(current_directory,"Product_Counts", f"{brand_name}_product_counts.csv")
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['URL', 'Product Count'])
        for url, count in url_product_counts:
            writer.writerow([url, count])

# Read the HTML file
file_path = r"C:\\Users\\User\\PycharmProjects\\WebScrapingUtility\\Web_Scraping_Utilities\\Web_Scraping_Utility\\Outputs\\Ferragamo\\2024-07-01 17-48-21\\shop\\us\\en\\men\\shoes-1.html"
try:
    with open(file_path, 'r', encoding='utf-8') as file:
        html = file.read()
    print("File read successfully")
except UnicodeDecodeError:
    print("UTF-8 decoding failed, trying with ISO-8859-1")
    with open(file_path, 'r', encoding='iso-8859-1') as file:
        html = file.read()
    print("File read successfully with ISO-8859-1 encoding")

# Debug information
print(f"Length of HTML content: {len(html)}")
print(f"First 500 characters of HTML: {html[:500]}")

# Count products
product_count = count_products(html, "r23-grid--list-plp__item__product-info", "li")
print(f"Total product count: {product_count}")

save_to_csv('Alexander_McQueen', [("https://www.alexandermcqueen.com/en-us/ca/men/accessories",product_count)])