import os
import json
import random
import time
from seleniumbase import SB
from lxml import html
from pymongo import MongoClient

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

def get_job_desc(links):
    job_desc_sources = []

    for link in links:
        proxy = get_random_proxy()
        print(f"Using proxy {proxy['url']} (errors so far: {proxy['num']})")
        try:
            with SB(test=True, uc=True, proxy=proxy["url"]) as sb:
                sb.activate_cdp_mode(link)
                sb.sleep(5)  # wait for page to load
                page_source = sb.get_page_source()
            job_desc_sources.append(page_source)
        except Exception as e:
            proxy["num"] += 1
            print(f"Error scraping {link} with proxy {proxy['url']}: {e}")
            continue

        time.sleep(2)  # small delay between requests

    final_data = []
    for page_source in job_desc_sources:
        tree = html.fromstring(page_source)
        job_data = {}
        try:
            job_div = tree.xpath('//div[@data-test="JobListing"]')[0]

            # Company name
            company = job_div.xpath(
                './/span[contains(@class, "text-sm font-semibold text-black")]/text()'
            )
            job_data["company_name"] = company[0].strip() if company else None

            # Hiring status
            hiring_status = job_div.xpath(
                ".//div[contains(@class, 'flex items-center text-sm font-medium text-pop-green')]/text()"
            )
            job_data["hiring_stat"] = hiring_status

            # Slogan
            slogan = job_div.xpath(
                ".//div[contains(@class, 'text-sm font-light text-neutral-500')]/text()"
            )
            job_data["slogan"] = slogan

            # Position title
            position = job_div.xpath(
                './/h1[contains(@class, "inline text-xl font-semibold text-black")]/text()'
            )
            job_data["position"] = position[0].strip() if position else None

            # Price, location, experience
            ul = job_div.xpath(
                './/ul[contains(@class, "block text-md text-black md:flex")]'
            )[0]
            price = ul.xpath('./li[1]/text()')
            job_data["price"] = price[0].strip() if price else None

            location = ul.xpath(
                './li[2]//a[contains(@class, "font-normal text-black text-md")]/text()'
            )
            job_data["location"] = location[0].strip() if location else None

            experience = ul.xpath('./li[3]/text()')
            if experience:
                job_data["experience_required"] = experience[0].replace('|', '').strip()
            else:
                job_data["experience_required"] = None

            # Visa Sponsorship
            visa_block = tree.xpath(
                '//span[contains(@class, "text-md font-semibold") and text()="Visa Sponsorship"]'
                '/following-sibling::p[1]/span/text()'
            )
            job_data["visa"] = visa_block[0].strip() if visa_block else None

            # Remote Work Policy
            remote_block = tree.xpath(
                '//span[contains(@class, "text-md font-semibold") and text()="Remote Work Policy"]'
                '/following-sibling::p[1]/text()'
            )
            job_data["remote_work_pol"] = remote_block[0].strip() if remote_block else None

            # Relocation
            relocation_block = tree.xpath(
                '//span[contains(@class, "text-md font-semibold") and text()="Relocation"]'
                '/following-sibling::span[@class="flex items-center"]/text()'
            )
            job_data["relocation"] = relocation_block[0].strip() if relocation_block else None

            # Skills
            skills_divs = tree.xpath(
                '//span[contains(@class, "text-md font-semibold") and text()="Skills"]'
                '/following-sibling::div[@class="flex flex-wrap"]/div/text()'
            )
            job_data["skills"] = [s.strip() for s in skills_divs if s.strip()]

            # Job description
            desc_div = tree.xpath('//div[@id="job-description"]')
            if desc_div:
                job_data["job_description"] = ' '.join(desc_div[0].xpath('.//text()')).strip()
            else:
                job_data["job_description"] = None

            # Additional key/value blocks
            div_a_selector = (
                "div.grid.grid-cols-2.items-center.gap-2.border-t.border-gray-300.p-4."
                "xs\\:flex.xs\\:flex-wrap"
            )
            for div_a in tree.cssselect(div_a_selector):
                div_b_selector = (
                    "div.flex.items-center.justify-center.rounded.bg-gray-200.px-2.font-medium"
                )
                for div_b in div_a.cssselect(div_b_selector):
                    imgs = div_b.cssselect("img")
                    if not imgs:
                        continue
                    key = imgs[0].get("alt")
                    inner = div_b.cssselect(
                        "div.flex.h-\\[20px\\].items-center.justify-center.space-x-1."
                        "whitespace-nowrap.pl-1.text-\\[10px\\].font-medium.leading-relaxed."
                        "text-accent-persian-600"
                    )
                    if inner:
                        value = inner[0].text_content().strip()
                        job_data.setdefault(key, []).append(value)

            # More additional data
            div_c = tree.cssselect(
                "div.flex.w-full.space-x-2.text-\\[10px\\].text-accent-persian-600."
                "border-t.border-gray-300.p-4.pb-4"
            )
            if div_c:
                items = div_c[0].cssselect("ul div.line-clamp-1")
                job_data["additional_data"] = [i.text_content().strip() for i in items]
            else:
                job_data["additional_data"] = []

            # Perks
            perks = []
            for div_a in tree.cssselect("div.grid.w-full.grid-cols-1.gap-4.sm\\:grid-cols-3.max-xs\\:grid-cols-2"):
                for div_b in div_a.cssselect("div.mt-4.flex.items-start"):
                    text = div_b.cssselect("div.h-full.pl-2")[0].text_content().strip()
                    perks.append(text)
            job_data["perks"] = perks

            # Founder info
            founder_el = tree.cssselect('div.mt-4.flex.rounded-lg.bg-brand-burgandy.p-4.text-center')[0]
            for div_b in founder_el.cssselect('div.w-1\\/2.border-r.border-white'):
                key = div_b.cssselect('div.text-xs.text-gray-400')[0].text_content().strip()
                value = div_b.cssselect('div.text-m.font-semibold.text-white')[0].text_content().strip()
                job_data[key] = value

            span_el = tree.cssselect("span.text-lg.font-semibold.text-black")
            job_data["founder"] = span_el[0].text_content().strip() if span_el else None

            final_data.append(job_data)
        except Exception as e:
            print("Skipping page due to parse error:", e)

    return final_data

def save_to_mongodb(data):
    """
    Connects to the local MongoDB server and inserts the given job data into
    the 'jobs' collection of the 'job_scraper' database.
    """
    try:
        client = MongoClient("mongodb://localhost:27017")
        db = client["job_scraper"]
        collection = db["jobs"]
        if data:
            result = collection.insert_many(data)
            print("Data inserted with ids:", result.inserted_ids)
        else:
            print("No data to insert")
    except Exception as e:
        print("Error connecting to MongoDB:", e)

if __name__ == "__main__":
    # Example list of job links to scrape
    job_links = [
        "https://wellfound.com/jobs/3259903-staff-software-engineer-metrics-us-remote"
    ]
    final_data = get_job_desc(job_links)

    # Save scraped data to a JSON file
    with open("data.json", "w") as f:
        json.dump(final_data, f, indent=4)

    # Insert into MongoDB
    save_to_mongodb(final_data)

    # Print proxy error counts for diagnostics
    print("\nProxy error counts:")
    for p in PROXIES:
        print(f" - {p['url']}: {p['num']}")
