import os
import json
import random
import time
from seleniumbase import SB
from lxml import html
from pymongo import MongoClient, errors

# ——— Config paths ———
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "config")
PROXY_FILE = os.path.join(CONFIG_DIR, "proxy.json")
URLS_FILE  = os.path.join(CONFIG_DIR, "urls.json")

# ——— Load & normalize proxies ———
try:
    raw_proxies = json.load(open(PROXY_FILE, "r"))
except Exception as e:
    raise RuntimeError(f"Could not load proxy list from {PROXY_FILE}: {e}")

proxy_urls = []
for entry in raw_proxies:
    if isinstance(entry, str):
        proxy_urls.append(entry)
    elif isinstance(entry, dict) and "url" in entry:
        proxy_urls.append(entry["url"])
    else:
        raise RuntimeError(f"Invalid proxy entry in {PROXY_FILE}: {entry}")

PROXIES = [{"url": url, "num": 0} for url in proxy_urls]
def get_random_proxy():
    return random.choice(PROXIES)

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
client     = MongoClient("mongodb://localhost:27017/")
db         = client["job_scraping"]
collection = db["job_urls"]
collection.create_index("url", unique=True)

# ——— Human‐like scrolling ———
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

# ——— Scrape all pages for one base_url ———
def scrape_pages_for_url(base_url):
    page = 1
    while True:
        page_url = base_url if page == 1 else f"{base_url}?page={page}"
        print(f"\nScraping page: {page_url}")

        for attempt in range(1, 4):
            proxy = get_random_proxy()
            print(f" Attempt {attempt} via proxy {proxy['url']} (errors={proxy['num']})")
            try:
                with SB(test=True, uc=True, proxy=proxy["url"]) as sb:
                    sb.activate_cdp_mode(page_url)
                    simulate_human_behavior(sb)
                    html_src = sb.get_page_source()

                # 404 check
                if "Page not found (404)" in html_src:
                    print("  ▶ 404 detected on this URL; saving and skipping.")
                    bad = page_url
                    try:
                        collection.update_one(
                            {"url": bad},
                            {"$setOnInsert": {"url": bad, "processed": False}},
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
                tree  = html.fromstring(html_src)
                hrefs = tree.xpath(
                    '//a[contains(@class, "mr-2") and contains(@class, "text-brand-burgandy")]/@href'
                )
                urls  = [
                    (base_url.split("/role")[0] + href) if href.startswith("/") else href
                    for href in hrefs
                ]

                if not urls:
                    print("  ▶ No job URLs found on this page.")
                    # Let it retry up to 3 times, then skip page
                else:
                    for full_url in urls:
                        try:
                            collection.update_one(
                                {"url": full_url},
                                {"$setOnInsert": {"url": full_url, "processed": False}},
                                upsert=True
                            )
                            print("   →", full_url)
                        except errors.PyMongoError as e:
                            print("   ⚠️ Mongo error:", e)
                    # successful scrape for this page, break retry loop
                    break

            except Exception as e:
                proxy["num"] += 1
                print(f"  ⚠️ Proxy error: {e}")
                time.sleep(random.uniform(2, 4))

        else:
            # ran out of attempts
            print("  ▶ Skipping this page after 3 failed attempts.")

        page += 1
        time.sleep(random.uniform(2, 4))

# ——— Main flow: loop through all URLs.json entries ———
if __name__ == "__main__":
    targets = load_target_urls()

    for entry in targets:
        if not entry.get("value", False):
            print(f"\n=== Starting scrape for: {entry['url']} ===")
            scrape_pages_for_url(entry["url"])
            entry["value"] = True
            save_target_urls(targets)
            print(f"=== Finished; marked {entry['url']} as done ===")

    total = collection.count_documents({})
    print(f"\n✅ All done! Total URLs in MongoDB: {total}")
    print("Proxy error counts:")
    for p in PROXIES:
        print(f" - {p['url']}: {p['num']}")
