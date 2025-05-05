import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import time
from itertools import islice
import json


df = pd.read_excel(r"D:\deproject\productid.xlsx")
product_id = df.iloc[:, 0].dropna().astype(int).tolist()

# Chia 1000 ID 1
def chunks(iterable, size):
    iterator = iter(iterable)
    for first in iterator:
        yield [first] + list(islice(iterator, size - 1))

id_chunked = list(chunks(product_id, 1000))

# làm sạch html
def clean_description(raw_html):
    if raw_html:
        soup = BeautifulSoup(raw_html, "html.parser")
        return soup.get_text(separator=" ", strip=True)
    return ""

# lấy data
def extract_product_info(data):
    images_list = []
    if "images" in data and isinstance(data["images"], list):
        images_list = [img.get("base_url") for img in data["images"] if img.get("base_url")]
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "url_key": data.get("url_key"),
        "price": data.get("price"),
        "description": clean_description(data.get("description")),
        "images_url": images_list
    }


async def run_program(pid, session, semaphore):
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{pid}"
    headers_async = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }

    is_429 = False

    async with semaphore:
        for attempt in range(2):  # Retry 2n lần
            try:
                async with session.get(url, headers=headers_async, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        return extract_product_info(data), is_429
                    elif response.status == 429:
                        is_429 = True
                        wait_time = (attempt + 1) * 5
                        await asyncio.sleep(wait_time)
                    else:
                        return None, is_429
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
                await asyncio.sleep(2)
            except Exception:
                return None, is_429

    return None, is_429


async def main():
    semaphore = asyncio.Semaphore(30)
    total_success = 0
    total_429 = 0

    async with aiohttp.ClientSession() as session:
        for idx, group in enumerate(id_chunked[:200]):
            tasks = [run_program(pid, session, semaphore) for pid in group]
            results = await asyncio.gather(*tasks)

            products_data = []
            count_429_in_group = 0

            for product_info, is_429 in results:
                if product_info:
                    products_data.append(product_info)
                if is_429:
                    count_429_in_group += 1


            with open(f"products_chunk_{idx}.json", "w", encoding="utf-8") as f:
                json.dump(products_data, f, ensure_ascii=False, indent=2)

            total_success += len(products_data)
            total_429 += count_429_in_group

    return total_success, total_429


start_time = time.time()

success_count, total_429 = asyncio.run(main())

end_time = time.time()
elapsed_time = end_time - start_time

print(f"Tổng sp thành công: {success_count}")
print(f"Tổng sp lỗi 429: {total_429}")
print(f"Tổng thời gian: {elapsed_time:.2f}")
