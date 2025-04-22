import os
import json
import random
import time
from seleniumbase import SB
from lxml import html
from pymongo import MongoClient, errors

# ——— Load proxies from config/proxy.json ———
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "config")
PROXY_FILE = os.path.join(CONFIG_DIR, "proxy.json")

try:
    with open(PROXY_FILE, "r") as f:
        proxy_urls = json.load(f)
except Exception as e:
    raise RuntimeError(f"Could not load proxy list from {PROXY_FILE}: {e}")

# Wrap each URL in a dict with an error counter
PROXIES = [{"url": url, "num": 0} for url in proxy_urls]

def get_random_proxy():
    return random.choice(PROXIES)

# ——— MongoDB setup ———
client = MongoClient("mongodb://localhost:27017/")
db = client["job_scraper_db"]
collection = db["job_urls"]
collection.create_index("url", unique=True)

def simulate_human_behavior(sb):
    sb.sleep(random.uniform(3, 5))
    for _ in range(random.randint(3, 6)):
        sb.cdp.scroll_down(random.randint(100, 400))
        sb.sleep(random.uniform(1, 2))
        if random.choice([True, False]):
            sb.cdp.scroll_up(random.randint(50, 150))
            sb.sleep(random.uniform(1, 2))
    try:
        height = sb.execute_script("return document.body.scrollHeight;")
        sb.execute_script(f"window.scrollTo(0, {random.randint(0, height)});")
    except Exception:
        pass
    sb.sleep(random.uniform(3, 6))

def scrape_all_job_urls():
    base_url = "https://wellfound.com"
    page = 1

    while True:
        page_url = (
            f"{base_url}/location/united-states"
            if page == 1
            else f"{base_url}/location/united-states?page={page}"
        )
        print(f"\nScraping page: {page_url}")

        found_any = False
        for attempt in range(1, 4):
            proxy = get_random_proxy()
            print(f" Attempt {attempt} using proxy {proxy['url']} (errors: {proxy['num']})")

            try:
                with SB(test=True, uc=True, proxy=proxy["url"]) as sb:
                    sb.activate_cdp_mode(page_url)
                    simulate_human_behavior(sb)
                    page_source = sb.get_page_source()

                if "Page not found (404)" in page_source:
                    print("  ▶ 404 detected. Ending scrape.")
                    return

                tree = html.fromstring(page_source)
                hrefs = tree.xpath(
                    '//a[contains(@class, "mr-2") and contains(@class, "text-brand-burgandy")]/@href'
                )
                urls = [
                    base_url + href if href.startswith("/") else href
                    for href in hrefs
                ]

                if not urls:
                    print("  ▶ No URLs found on this attempt.")
                else:
                    for full_url in urls:
                        print("   →", full_url)
                        try:
                            collection.update_one(
                                {"url": full_url},
                                {"$setOnInsert": {"url": full_url, "processed": False}},
                                upsert=True
                            )
                        except errors.PyMongoError as e:
                            print("   ⚠️ Mongo insert error:", e)
                    found_any = True
                    break

            except Exception as e:
                # increment error count for this proxy
                proxy["num"] += 1
                print(f"  ⚠️ Error with proxy {proxy['url']}: {e}")
                # wait a bit before retry
                time.sleep(random.uniform(2, 4))

        if not found_any:
            print("  ▶ No URLs found after 3 attempts; moving on.")
        page += 1
        time.sleep(random.uniform(2, 4))

if __name__ == "__main__":
    scrape_all_job_urls()
    total = collection.count_documents({})
    print(f"\nTotal URLs stored in MongoDB: {total}")
    # Optionally: print proxy error counts
    print("Proxy error counts:")
    for p in PROXIES:
        print(f" - {p['url']}: {p['num']}")
