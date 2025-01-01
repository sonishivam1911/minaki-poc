import aiohttp
import asyncio
import os
from bs4 import BeautifulSoup
from config.settings import BASE_URL

async def download_image(session, img_url, output_folder, retries=3):
    """
    Downloads a single image asynchronously with optional retry logic.
    Returns the local file path if successful, or None if it fails.
    """
    for attempt in range(1, retries + 1):
        try:
            async with session.get(img_url) as response:
                if response.status == 200:
                    filename = os.path.basename(img_url)
                    filepath = os.path.join(output_folder, filename)
                    with open(filepath, 'wb') as f:
                        f.write(await response.read())
                    return filepath  # Success
                else:
                    print(f"[Attempt {attempt}/{retries}] Failed to download "
                          f"{img_url} (Status {response.status})")
        except Exception as e:
            print(f"[Attempt {attempt}/{retries}] Error downloading {img_url}: {e}")
        
        if attempt < retries:
            # Wait a bit before retrying
            await asyncio.sleep(1)
    
    return None  # All retries failed

async def download_images_in_batches(session, img_urls, output_folder, batch_size=20):
    """
    Downloads images asynchronously in batches of 'batch_size'.
    Uses return_exceptions=True so partial failures don't cancel the batch.
    
    Returns a list of file paths (or None for failed downloads).
    """
    all_results = []
    for i in range(0, len(img_urls), batch_size):
        batch = img_urls[i : i + batch_size]
        tasks = [
            download_image(session, img_url, output_folder) 
            for img_url in batch
        ]
        # Gather with return_exceptions to keep going if one fails
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in batch_results:
            if isinstance(result, Exception):
                # Log the exception (we won't raise it to avoid cancellation)
                print(f"Image download exception: {result}")
                all_results.append(None)
            else:
                # result is a file path or None
                all_results.append(result)
    return all_results

async def process_product_page(session, link, sku_folder, retries=3):
    """
    Processes a single product page (with optional retry).
    1. Fetches the product page HTML.
    2. Finds image tags in '.prod_desc_left.fl'.
    3. Downloads them in batches of 20.
    
    Returns True if it downloaded at least one image, False if it failed or found none.
    """
    for attempt in range(1, retries + 1):
        try:
            async with session.get(link) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    prod_desc_left = soup.find('div', class_='prod_desc_left fl')
                    if not prod_desc_left:
                        print(f"No image container found on page: {link}")
                        return False
                    
                    img_tags = prod_desc_left.find_all('img')
                    if not img_tags:
                        print(f"No <img> tags found on page: {link}")
                        return False
                    
                    # Construct absolute URLs
                    img_urls = [
                        (BASE_URL + img['src']) if not img['src'].startswith(BASE_URL)
                        else img['src']
                        for img in img_tags if 'src' in img.attrs
                    ]
                    
                    # Download images in sub-batches of 20
                    downloaded_files = await download_images_in_batches(session, img_urls, sku_folder, batch_size=20)
                    
                    # Check if we got at least one success
                    if any(downloaded_files):
                        return True
                    else:
                        print(f"No images actually saved for page: {link}")
                        return False
                else:
                    print(f"[Attempt {attempt}/{retries}] Failed to fetch page: {link} "
                          f"(Status {response.status})")
        except Exception as e:
            print(f"[Attempt {attempt}/{retries}] Error processing page {link}: {e}")

        if attempt < retries:
            print(f"Retrying {link} after short delay...")
            await asyncio.sleep(1)
    
    return False  # All retries exhausted

async def process_batch(products_df_batch):
    """
    Processes a subset (batch) of products. We:
      - Create tasks for each SKU
      - Process the tasks concurrently.
      - Return a list of failed SKUs at the end.
    """
    failed_skus = []
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        
        for _, row in products_df_batch.iterrows():
            link = row.get('link')
            sku = row.get('Generated SKU')
            
            if not link or not sku:
                print(f"Skipping product with missing link/SKU: {link} / {sku}")
                failed_skus.append(sku)
                continue
            
            folder = os.path.join("vendor_images", sku)
            os.makedirs(folder, exist_ok=True)
            tasks.append(process_product_page(session, link, folder))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Evaluate results
        for i, result in enumerate(results):
            sku = products_df_batch.iloc[i]['Generated SKU']
            if isinstance(result, Exception) or not result:
                failed_skus.append(sku)
    
    return failed_skus
