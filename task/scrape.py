import aiohttp
import aiofiles
import asyncio
import json
import os
import random
from bs4 import BeautifulSoup
from typing import List
import logging
import bs4

BASE_URL = 'https://peraturan.bpk.go.id/Search?keywords=&tentang=&nomor=&p=1'
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:131.0) Gecko/20100101 Firefox/131.0'
}
TEMP_DIR = 'temp_laws'  # Directory to store temporary JSON files
os.makedirs(TEMP_DIR, exist_ok=True)  # Create directory if it doesn't exist

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO or DEBUG as needed
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Log to stdout
    ]
)
logger = logging.getLogger(__name__)

async def get_num_pages(url: str) -> int:
    
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(BASE_URL) as resp:
            soup = BeautifulSoup(await resp.text(), 'html.parser')
            last_page_url = list(soup.select('.page-link'))[-1].get('href')
            last_page_number = int(last_page_url.split('p=')[-1])
    return last_page_number


def process_soup_to_law_list(soup: BeautifulSoup) -> List[dict]:
    list_law = []
    for law_item in list(soup.select_one('.rounded-4').children):
        if not (law_item and isinstance(law_item, bs4.Tag) and all(cls in law_item.get('class', []) for cls in ['row', 'mb-8'])):
            continue
        title = law_item.select_one('.col-lg-8.fw-semibold.fs-5.text-gray-600')
        if title:
            title = title.get_text().strip()
        about = law_item.select_one('.col-lg-10.fs-2.fw-bold.pe-4')
        if about:
            about = about.get_text().strip()
        category = law_item.select_one('.badge.badge-light-primary.mb-2')
        if category:
            category= category.get_text().strip()
        detail_url = law_item.select_one('.col-lg-10.fs-2.fw-bold.pe-4')
        if detail_url:
            detail_url = detail_url.select_one('a').get('href')
        download_urls = []
        for url in list(law_item.select('.download-file.text-danger.text-hover-primary')):
            download_urls.append(url.get('href'))
        statuses = []
        for status_item in law_item.select('.row.g-4.g-xl-9.mb-8'):
            status_name = status_item.select_one('.col-lg-2').get_text().strip()
            status_associated_uu = []
            for associated_uu in list(status_item.select('.text-start.mb-2')):
                if associated_uu.select_one('.text-danger'):
                    status_associated_uu.append({
                        'name':associated_uu.select_one('.text-danger').get_text().strip().replace('\n', ''),
                        'url': associated_uu.select_one('.text-danger').get('href')
                        })
                else:
                    status_associated_uu.append({
                        'name':associated_uu.get_text().strip().replace('\n', ''),
                        'url':None
                        })
            statuses.append({
                'name': status_name,
                'associated_uus': status_associated_uu
            })
        modal = law_item.find_next_sibling('div')
        abstract = modal.get_text().strip() if modal else ""
        list_law.append({
            'title': title,
            'about': about,
            'category': category,
            'detail_url': detail_url,
            'download_urls': download_urls,
            'statuses': statuses,
            'abstract':abstract
        })

    for idx, _ in enumerate(list_law):
        url = list_law[idx]['detail_url']

        #identifying region
        law_name_identifier = url.split('/')[-1]
        if '-kab-' in law_name_identifier:
            region = 'kab-'+law_name_identifier.split('-kab-')[-1].split('-no-')[0]
        elif '-kota-' in law_name_identifier:
            region = 'kota-'+law_name_identifier.split('-kota-')[-1].split('-no-')[0]
        elif '-prov-' in law_name_identifier:
            region = 'prov-'+law_name_identifier.split('-prov-')[-1].split('-no-')[0]
        else:
            region = 'nasional'
        
        if 'tahun-' in law_name_identifier:
            year = law_name_identifier.split('tahun-')[-1]
        else:
            year = None
        list_law[idx] = {
            "id": int(url.split('/')[2]) if url else None,
            "type": url.split('/')[-1].split('-')[0] if url else None,
            "region": region,
            "year":year,
            **list_law[idx]
        }
        for idx_status, status in enumerate(list_law[idx]['statuses']):
            for idx_associated_uu, associated_uu in enumerate(status['associated_uus']):
                url_associated_uu = list_law[idx]['statuses'][idx_status]['associated_uus'][idx_associated_uu]['url']
                list_law[idx]['statuses'][idx_status]['associated_uus'][idx_associated_uu] = {
                    "id": int(url_associated_uu.split('/')[2]) if url_associated_uu else None,
                    **list_law[idx]['statuses'][idx_status]['associated_uus'][idx_associated_uu]
                }

    return list_law

async def fetch_law_list(url: str, page_num: int, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> None:
    temp_file_path = os.path.join(TEMP_DIR, f'page_{page_num}.json')
    
    # Skip fetching if temporary JSON already exists
    if os.path.exists(temp_file_path):
        logger.warning(f"Skipping page {page_num}, temporary file already exists.")
        return
    
    max_retries = 5
    retry_count = 0
    while retry_count < max_retries:
        try:
            async with semaphore:  # Limit concurrency
                async with session.get(url, timeout=60) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get('Retry-After', 2 ** retry_count))
                        delay = retry_after + random.uniform(1, 3)
                        logger.warning(f"429 Too Many Requests. Retrying {retry_count + 1}/{max_retries} after {delay} seconds...")
                        await asyncio.sleep(delay)
                        retry_count += 1
                        continue
                    soup = BeautifulSoup(await resp.text(), 'html.parser')
                    list_law = process_soup_to_law_list(soup)
                    
                    # Asynchronously save the processed data to a temporary JSON file
                    async with aiofiles.open(temp_file_path, 'w') as f:
                        await f.write(json.dumps(list_law))
                    logger.info(f"Successfully saved page {page_num} to {temp_file_path}")
                    break
        except Exception as e:
            retry_count += 1
            delay = min(2 ** retry_count,10)
            logger.error(f"Error fetching {url}: {e}. Delaying {delay} seconds")
            await asyncio.sleep(delay)

async def get_law_list_from_page(num_pages: int) -> List[dict]:
    semaphore = asyncio.Semaphore(50)  # Limit to 5 concurrent requests
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [
            fetch_law_list(
                url=f'https://peraturan.bpk.go.id/Search?keywords=&tentang=&nomor=&p={i}', 
                page_num=i, 
                session=session, 
                semaphore=semaphore
            ) for i in range(1, num_pages + 1)
        ]
        await asyncio.gather(*tasks)

async def merge_temp_files() -> List[dict]:
    list_law = []
    for filename in os.listdir(TEMP_DIR):
        if filename.endswith('.json'):
            async with aiofiles.open(os.path.join(TEMP_DIR, filename), 'r') as f:
                data = await f.read()
                print(filename)
                try:
                    page_data = json.loads(data)
                except Exception as e:
                    page_data = []
                    logger.warning(f"Skipping {filename} as it contains error: {e}")
                list_law.extend(page_data)
    return list_law

async def main():
    last_page_number = await get_num_pages(BASE_URL)
    last_page_number = 5
    # Main execution
    await get_law_list_from_page(last_page_number)

    # Merge all temporary JSON files into a single file
    merged_list_law = await merge_temp_files()
    async with aiofiles.open('list_law.json', 'w') as f:
        await f.write(json.dumps(merged_list_law, indent=2))
    logger.info("All pages have been merged into list_law.json")

if __name__ == "__main__":
    asyncio.run(main())
