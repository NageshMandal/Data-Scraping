import os
import json
import random
import time
import requests
from lxml import html
from pymongo import MongoClient, errors
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ——— ZenRows Configuration ———
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")
if not ZENROWS_API_KEY:
    raise RuntimeError("ZENROWS_API_KEY not found in .env file")

ZENROWS_API_URL = "https://api.zenrows.com/v1/"

def zenrows_request(url, js_render=True, premium_proxy=True, block_resources="image,stylesheet,font,media"):
    """
    Make a request through ZenRows API with anti-bot measures
    """
    params = {
        'apikey': ZENROWS_API_KEY,
        'url': url,
        'js_render': str(js_render).lower(),
        'premium_proxy': str(premium_proxy).lower(),
        'wait': '3000',  # Wait 3 seconds for page load
        'wait_for': '.text-brand-burgandy',  # Wait for job links to appear
    }
    
    # Add block_resources only if specified (ZenRows uses specific format)
    if block_resources:
        params['block_resources'] = block_resources
    
    print(f"🔄 ZenRows request: {url}")
    
    try:
        response = requests.get(ZENROWS_API_URL, params=params, timeout=90)
        
        if response.status_code == 200:
            print(f"✅ Success - Response length: {len(response.text)}")
            return response.text
        elif response.status_code == 422:
            print("⚠️ ZenRows: Unprocessable request - might be blocked or invalid URL")
            return None
        elif response.status_code == 429:
            print("⚠️ ZenRows: Rate limit hit - waiting...")
            time.sleep(10)
            return None
        else:
            print(f"❌ ZenRows error: {response.status_code} - {response.text}")
            return None
            
    except requests.exceptions.Timeout:
        print("⏰ ZenRows request timeout")
        return None
    except Exception as e:
        print(f"❌ ZenRows request failed: {e}")
        return None

# ——— Config paths ———
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "config")
URLS_FILE = os.path.join(CONFIG_DIR, "urls.json")

# ——— Load & save target‐URLs state ———
def load_target_urls():
    try:
        return json.load(open(URLS_FILE, "r"))
    except Exception as e:
        raise RuntimeError(f"Could not load URLs from {URLS_FILE}: {e}")

def save_target_urls(urls):
    with open(URLS_FILE, "w") as f:
        json.dump(urls, f, indent=2)

# ——— MongoDB setup ———
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db["job_urls"]
collection.create_index("url", unique=True)

# ——— Scrape all pages for one base_url ———
def scrape_pages_for_url(base_url):
    page = 1
    while True:
        page_url = base_url if page == 1 else f"{base_url}?page={page}"
        print(f"\n🕷️ Scraping page {page}: {page_url}")

        # Try up to 3 times with ZenRows
        for attempt in range(1, 4):
            print(f"  Attempt {attempt}/3")
            
            html_src = zenrows_request(page_url)
            
            if html_src is None:
                print(f"  ⚠️ Attempt {attempt} failed")
                if attempt < 3:
                    time.sleep(random.uniform(3, 7))
                continue
                
            # 404 check
            if "Page not found (404)" in html_src:
                print("  ▶ 404 detected on this URL; saving and skipping.")
                try:
                    collection.update_one(
                        {"url": page_url},
                        {"$setOnInsert": {"url": page_url, "processed": False}},
                        upsert=True
                    )
                except errors.PyMongoError as e:
                    print("   ⚠️ Mongo error on 404-save:", e)
                return

            # zero-results check
            if "0 results total" in html_src:
                print("  ▶ Zero results for this URL; skipping.")
                return

            # normal scrape
            try:
                tree = html.fromstring(html_src)
                hrefs = tree.xpath(
                    '//a[contains(@class, "mr-2") and contains(@class, "text-brand-burgandy")]/@href'
                )
                urls = [
                    (base_url.split("/role")[0] + href) if href.startswith("/") else href
                    for href in hrefs
                ]

                if not urls:
                    print("  ▶ No job URLs found on this page.")
                    if attempt < 3:
                        continue
                    else:
                        print("  ▶ No URLs found after 3 attempts, moving to next page.")
                        break
                else:
                    print(f"  ✅ Found {len(urls)} job URLs")
                    for full_url in urls:
                        try:
                            collection.update_one(
                                {"url": full_url},
                                {"$setOnInsert": {"url": full_url, "processed": False, "scraped": False}},
                                upsert=True
                            )
                            print(f"    → {full_url}")
                        except errors.PyMongoError as e:
                            print(f"    ⚠️ Mongo error: {e}")
                    # successful scrape for this page, break retry loop
                    break
                    
            except Exception as e:
                print(f"  ❌ HTML parsing error: {e}")
                if attempt < 3:
                    continue
                    
        else:
            # ran out of attempts
            print("  ▶ Skipping this page after 3 failed attempts.")
            break

        page += 1
        # Be polite between pages
        sleep_time = random.uniform(2, 5)
        print(f"  😴 Sleeping {sleep_time:.1f}s before next page...")
        time.sleep(sleep_time)

# ——— Main flow: loop through all URLs.json entries ———
if __name__ == "__main__":
    if not os.path.exists(URLS_FILE):
        print(f"❌ URLs file not found: {URLS_FILE}")
        print("Please run: python main.py --step generate_urls first")
        exit(1)
    
    targets = load_target_urls()
    print(f"📋 Loaded {len(targets)} target URLs")

    for i, entry in enumerate(targets, 1):
        if not entry.get("value", False):
            print(f"\n🎯 [{i}/{len(targets)}] Starting scrape for: {entry['url']}")
            scrape_pages_for_url(entry["url"])
            entry["value"] = True
            save_target_urls(targets)
            print(f"✅ Finished; marked {entry['url']} as done")
            
            # Be polite between different base URLs
            if i < len(targets):
                sleep_time = random.uniform(5, 10)
                print(f"😴 Sleeping {sleep_time:.1f}s before next base URL...")
                time.sleep(sleep_time)
        else:
            print(f"⏭️ [{i}/{len(targets)}] Skipping already processed: {entry['url']}")

    total = collection.count_documents({})
    unscraped = collection.count_documents({"scraped": False})
    print(f"\n🎉 All done! Total URLs in MongoDB: {total}")
    print(f"📊 URLs ready for job scraping: {unscraped}")
    print("🚀 Next step: python main.py --step scrape_jobs")
