import pandas as pd
import aiohttp
import asyncio
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


## Scraping functions ##
# 1. IDs
def get_product_id(full_content):
    product_id = []
    try:
        for product in full_content:
            href = product.find(
                'h3', class_='item-title').find('a').get('href')
            id_string = href.split('/')[-1].split('.')[0].split('_')[-1]
            product_id.append(int(id_string))
    except AttributeError:
        product_id = ""
    return product_id


# 2. Titles
def get_titles(full_content):
    titles = []
    try:
        for product in full_content:
            titles.append(product.find(
                "a", attrs={'class': 'subject'}).text.strip())
    except AttributeError:
        titles = ""
    return titles


# 3. Prices
def get_prices(full_content):
    prices = []
    try:
        for product in full_content:
            price_string = product.find(
                "span", attrs={'class': 'price-value'}).text.strip()
            if price_string:
                price_numeric = int(''.join(filter(str.isdigit, price_string)))
                prices.append(price_numeric)
            else:
                prices.append(0)
    except AttributeError:
        prices = 0
    return prices


# 4. Listed Dates
def get_listed_dates(full_content):
    listed_dates = []
    try:
        for product in full_content:
            date_element = product.find("div", attrs={'class': 'time'})
            if date_element is None:
                listed_dates.append("")
            else:
                listed_dates.append(date_element.text.strip())
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%b %d")
        today = datetime.now().strftime("%b %d")
        one_month_plus = 'more than 1 month'

        listed_dates = [yesterday if x.startswith(
            'tegnap') else x for x in listed_dates]
        listed_dates = [today if x.startswith(
            'ma') else x for x in listed_dates]
        listed_dates = [one_month_plus if x ==
                        'több, mint egy hónapja' else x for x in listed_dates]
        listed_dates = [s.split('.')[0] for s in listed_dates]
        listed_dates = [date.replace('márc', 'Mar').replace(
            'ápr', 'Apr').replace('máj', 'May') for date in listed_dates]
    except AttributeError:
        listed_dates = ""
    return listed_dates


# 5. Links
def get_links(full_content):
    links = []
    try:
        for product in full_content:
            links.append(product.find(
                'h3', class_='item-title').find('a').get('href'))
    except AttributeError:
        links = ""
    return links


# 6. Categories
def get_categories(full_content):
    categories = []
    try:
        for product in full_content:
            categories.append(product.find(
                "div", attrs={'class': 'category'}).text.strip())
    except AttributeError:
        categories = ""
    return categories


# 7. Locations
def get_locations(full_content):
    locations = []
    try:
        for product in full_content:
            location_element = product.find(
                "section", attrs={'class': 'reLiSection cityname'})
            if location_element is None:
                locations.append('')
            else:
                locations.append(location_element.text.strip())
    except AttributeError:
        locations = ""
    return locations


# 8. Delivery
def get_delivery(full_content):
    delivery = []
    try:
        for product in full_content:
            delivery_element = product.find(
                "section", attrs={'class': 'reLiSection badges'})
            if delivery_element is None:
                delivery.append('')
            else:
                delivery.append(delivery_element.text.strip())
        delivery = [data.replace('Üzleti\n', '(Üzleti) ') for data in delivery]
    except AttributeError:
        delivery = ""
    return delivery


# 9. Profile Images
def get_profile_images(full_content):
    profile_images = []
    try:
        for product in full_content:
            profile_images.append(product.select_one(
                'section.reLiSection.imageBox a img').get('src'))
    except AttributeError:
        profile_images = ""
    return profile_images


# 10. Searched Dates
def get_searched_date(full_content):
    searched_date = []
    for _ in full_content:
        searched_date.append(int(datetime.now().strftime("%Y%m%d")))
    return searched_date


# 11. Last page
def get_last_page(soup):
    last_page = []
    try:
        last_page = int(soup.find('a', {
                        'class': 'ad-list-pager-item-last'}).get('href').split('=')[-1])
    except AttributeError:
        last_page = 1
    return last_page


# Get Data async from URL
async def get_page(session, URL, PROXY, HEADERS, TIMEOUT):
    async with session.get(URL, headers=HEADERS, proxy=PROXY, timeout=TIMEOUT) as r:
        return await r.text()


async def get_all(session, URLs, PROXY, HEADERS, TIMEOUT):
    tasks = []
    for URL in URLs:
        task = asyncio.create_task(
            get_page(session, URL, PROXY, HEADERS, TIMEOUT))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results


async def main(URLs, PROXY, HEADERS, TIMEOUT):
    async with aiohttp.ClientSession() as session:
        data = await get_all(session, URLs, PROXY, HEADERS, TIMEOUT)
        return data


def get_full_content(results):
    full_content = []
    for r in results:
        try:
            soup = BeautifulSoup(r, "html.parser")
        except:
            soup = results
        ## Full Content including Ads ##
        full_content.extend(soup.find_all(
            "div", attrs={'class': 'contentArea'}))
        ## Full Content WITHOUT Ads ##
        # full_content = soup.find_all("div", attrs={'class':'col-xs-12 box listing list-item reListElement'})
    return full_content


# Get Data from URL
def get_data(keyword, PROXY, HEADERS, TIMEOUT):
    URL = [f'https://www.jofogas.hu/magyarorszag?q={keyword}&o=1']
    soup1 = asyncio.run(main(URL, PROXY, HEADERS, TIMEOUT))
    last_page = get_last_page(BeautifulSoup(soup1[0], "html.parser"))
    note = ""
    if last_page != 1:
        # Set maximum pages for scraping. We choose '5'
        max_page = 5
        if last_page > max_page:
            note = f'Note: Your search for "{keyword}" returned a large number of results, with a total of {last_page} pages. Consider using a more specific keyword for a search f.e. "iPhone 14 Pro 128GB" instead of "iPhone". We have provided the first 5 pages of this specific search.'
            last_page = max_page
        URL = []
        for number in range(2, last_page+1):
            URL.append(
                f'https://www.jofogas.hu/magyarorszag?q={keyword}&o={number}')
        soup2 = asyncio.run(main(URL, PROXY, HEADERS, TIMEOUT))
    else:
        soup2 = []

    soup = soup1 + soup2
    full_content = get_full_content(soup)
    full_product = {
        "Product ID": get_product_id(full_content),
        "Title": get_titles(full_content),
        "Price": get_prices(full_content),
        "Listed Date": get_listed_dates(full_content),
        "Link": get_links(full_content),
        "Category": get_categories(full_content),
        "Location": get_locations(full_content),
        "Delivery": get_delivery(full_content),
        "Image": get_profile_images(full_content),
        "Searched Date": get_searched_date(full_content),
        "Keyword Used": keyword
    }
    df_full_product = pd.DataFrame(full_product)
    df_full_product.dropna(subset=['Listed Date'], inplace=True)
    df_full_product = df_full_product[df_full_product['Price'] != 0]
    df_full_product.fillna(value='', inplace=True)
    df_full_product.replace("'", "", regex=True, inplace=True)
    df_full_product.index = df_full_product.index + 1
    return df_full_product, note


# Products to html table format
def products_to_html(full_product):
    full_product['Title'] = full_product.apply(
        lambda row: f'<a href="{row["Link"]}" target="_blank">{row["Title"]}</a>', axis=1)
    full_product['Image'] = full_product.apply(
        lambda row: f'<img src="{row["Image"]}" width="80" height="100"', axis=1)
    full_product['Price'] = full_product['Price'].apply(
        lambda x: '{:,}'.format(int(x)))
    full_product.drop(
        ['Product ID', 'Link', 'Searched Date', 'Keyword Used'], axis=1, inplace=True)
    full_product_html = full_product[[
        'Image', 'Title', 'Listed Date', 'Category', 'Location', 'Delivery', 'Price']]
    full_product_html = full_product_html.to_html(
        table_id="products", render_links=True, escape=False, classes='table table-striped')
    return full_product_html


# Connect to MySQL Server
def get_sql_connection():
    load_dotenv()
    db_engine = create_engine(os.getenv("SQLALCHEMY"))
    return db_engine


# Get data from MySQL Server to html table format
def get_from_mysql(engine, filter_name):
    with engine.begin() as conn:
        query = text(
            ""f"SELECT products.image, products.title, products.link, prices.* FROM jofogas.prices LEFT JOIN jofogas.products ON prices.product_id = products.product_id WHERE products.title LIKE '%{filter_name}%'""")
        df = pd.read_sql_query(query, conn)
    df['price_change'] = df['latest_price'] - df['initial_price']
    df['title'] = df.apply(
        lambda row: f'<a href="{row["link"]}" target="_blank">{row["title"]}</a>', axis=1)
    df['image'] = df.apply(
        lambda row: f'<img src="{row["image"]}" width="80" height="80"', axis=1)
    df['initial_search_date'] = pd.to_datetime(
        df['initial_search_date'], format='%Y%m%d')
    df['latest_search_date'] = pd.to_datetime(
        df['latest_search_date'], format='%Y%m%d')
    df[['initial_price', 'latest_price', 'price_change']] = df[['initial_price',
                                                                'latest_price', 'price_change']].applymap(lambda x: '{:,}'.format(int(x)))
    df = df.drop(['product_id', 'link'], axis=1)
    df = df.fillna(0)
    df_from_sql = pd.DataFrame({'Image': df['image'], 'Title': df['title'], 'Initial Price': df['initial_price'], 'Initial Search Date': df['initial_search_date'],
                                'Latest Price': df['latest_price'], 'Latest Search Date': df['latest_search_date'], 'Price Change': df['price_change']}, index=df.index)
    df_from_sql.index = df_from_sql.index + 1
    df_from_sql = df_from_sql.to_html(
        table_id="products", render_links=True, escape=False, classes='table table-striped')
    return df_from_sql


# Upload data to MySQL Server
def upload_to_mysql(engine, df_full_product):
    # Upload to 'products' table
    with engine.begin() as conn:
        query = text("""SELECT product_id FROM products""")
        df_sql_products = pd.read_sql_query(query, conn)
    df_products = df_full_product[~df_full_product['Product ID'].isin(
        df_sql_products['product_id'])].copy()
    df_products.rename(
        columns={'Listed Date': 'listed_date', 'Product ID': 'product_id'}, inplace=True)
    df_products[['product_id', 'Title', 'listed_date', 'Link', 'Category', 'Location',
                 'Delivery', 'Image']].to_sql('products', con=engine, if_exists='append', index=False)

    # Upload to 'prices' table.
    with engine.begin() as conn:
        query = text("""SELECT * FROM prices""")
        existing_ids = pd.read_sql_query(query, conn)
    df_prices = df_full_product.drop(['Title', 'Listed Date', 'Link', 'Category',
                                      'Location', 'Delivery', 'Image', 'Keyword Used'], axis=1).copy()
    df_prices.rename(columns={'Product ID': 'product_id', 'Price': 'initial_price',
                     'Searched Date': 'initial_search_date'}, inplace=True)
    df_prices['initial_price'] = df_prices['initial_price'].astype(int)

    # Insert product to 'prices' table if does not exist
    df_prices_insert = df_prices[~df_prices['product_id'].isin(
        existing_ids['product_id'])].copy()
    df_prices_insert[['latest_price', 'latest_search_date']] = df_prices_insert[[
        'initial_price', 'initial_search_date']].astype(int)
    df_prices_insert.to_sql(
        'prices', con=engine, if_exists='append', index=False)

    # Update product to 'prices' table if exist
    df_prices_update = df_prices[df_prices['product_id'].isin(
        existing_ids['product_id'])].copy()
    merged = pd.merge(existing_ids, df_prices_update, on='product_id')
    merged = merged[~((merged['initial_price_y'] == merged['latest_price']) & (
        merged['initial_search_date_y'] == merged['latest_search_date']))]
    merged['latest_price'] = merged['initial_price_y']
    merged['latest_search_date'] = merged['initial_search_date_y']
    merged.rename(columns={'initial_price_x': 'initial_price',
                  'initial_search_date_x': 'initial_search_date'}, inplace=True)
    merged = merged.drop(['initial_price_y', 'initial_search_date_y'], axis=1)
    for _, row in merged.iterrows():
        engine.execute(
            f"UPDATE prices SET latest_price={row['latest_price']}, latest_search_date={row['latest_search_date']} WHERE product_id={row['product_id']}")

    engine.dispose()
