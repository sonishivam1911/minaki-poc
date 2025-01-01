from bs4 import BeautifulSoup
import pandas as pd
from config.settings import BASE_URL, CATEGORY_MAPPING

def normalize_product_name(product_name):
    """Normalize product name by replacing non-standard parentheses with standard ones."""
    return product_name.replace("（", "(").replace("）", ")")

def parse_product_name(product_name):
    """Extract item name and vendor code from a product name string."""
    product_name = normalize_product_name(product_name)
    vendor_code = None
    item_name = None

    if '(' in product_name and ')' in product_name:
        vendor_code_start = product_name.find('(') + 1
        vendor_code_end = product_name.find(')')
        vendor_code = product_name[vendor_code_start:vendor_code_end].strip()

    if '(' in product_name:
        item_name = product_name.split('(')[0].strip()
    else:
        item_name = product_name.strip()

    return item_name, vendor_code

def extract_product_links(order_html):
    """Extract product links, names, vendor codes, and categories from the order HTML."""
    soup = BeautifulSoup(order_html, 'html.parser')
    order_summary = soup.find('div', class_='order_menu order_summary')

    product_links = []

    if order_summary:
        rows = order_summary.find_all('tr')
        for row in rows:
            link_tag = row.find('a', href=True)
            if link_tag:
                product_link = link_tag['href']
                if not product_link.startswith(BASE_URL):
                    product_link = BASE_URL + product_link

                product_name = link_tag.get('title', '')
                item_name, vendor_code = parse_product_name(product_name)

                category = None
                for key in CATEGORY_MAPPING.keys():
                    if key in item_name.lower():
                        category = CATEGORY_MAPPING[key]
                        break

                product_links.append({
                    'link': product_link,
                    'item_name': item_name,
                    'vendor_code': vendor_code,
                    'category': category
                })

    return pd.DataFrame(product_links)
