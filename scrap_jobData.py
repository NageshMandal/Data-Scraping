import os
import random
import json
import time
from seleniumbase import SB
from lxml import html
from pymongo import MongoClient

# ——— Proxy configuration ———
PROXY_LIST = [
    "mfqzbvxw:j67ah2zbwasb@38.153.152.244:9594",
    "mfqzbvxw:j67ah2zbwasb@86.38.234.176:6630",
    "mfqzbvxw:j67ah2zbwasb@173.211.0.148:6641",
    "mfqzbvxw:j67ah2zbwasb@161.123.152.115:6360",
    "mfqzbvxw:j67ah2zbwasb@216.10.27.159:6837",
    "mfqzbvxw:j67ah2zbwasb@154.36.110.199:6853",
    "mfqzbvxw:j67ah2zbwasb@45.151.162.198:6600",
    "mfqzbvxw:j67ah2zbwasb@185.199.229.156:7492",
    "mfqzbvxw:j67ah2zbwasb@185.199.228.220:7300",
    "mfqzbvxw:j67ah2zbwasb@185.199.231.45:8382",
]

def get_random_proxy():
    return random.choice(PROXY_LIST)

def get_job_desc(links):
    job_desc_sources = []
    for link in links:
        proxy = get_random_proxy()
        print(f"→ Using proxy {proxy}")
        with SB(test=True, uc=True, proxy=proxy) as sb:
            sb.open(link)
            sb.sleep(5)
            # optional: try to bypass captcha
            try:
                sb.sleep(10)
            except Exception as e:
                print("No captcha:", e)
            ps = sb.get_page_source()
            job_desc_sources.append(ps)
            sb.sleep(2)

    final_data = []
    for page_source in job_desc_sources:
        tree = html.fromstring(page_source)
        job_data = {}
        try:
            job_div = tree.xpath('//div[@data-test="JobListing"]')[0]

            company = job_div.xpath('.//span[contains(@class, "text-sm font-semibold text-black")]/text()')
            job_data["company_name"] = company[0].strip() if company else None

            print(job_data["company_name"])

            hiring_status = job_div.xpath(".//div[contains(@class, 'flex items-center text-sm font-medium text-pop-green')]/text()")
            job_data["hiring_stat"] = hiring_status

            slogan = job_div.xpath(".//div[contains(@class, 'text-sm font-light text-neutral-500')]/text()")
            job_data["slogan"] = slogan

            position = job_div.xpath('.//h1[contains(@class, "inline text-xl font-semibold text-black")]/text()')
            job_data["position"] = position[0].strip() if position else None

            ul = job_div.xpath('.//ul[contains(@class, "block text-md text-black md:flex")]')[0]

            price = ul.xpath('./li[1]/text()')
            job_data["price"] = price[0].strip() if price else None

            location = ul.xpath('./li[2]//a[contains(@class, "font-normal text-black text-md")]/text()')
            job_data["location"] = location[0].strip() if location else None

            experience = ul.xpath('./li[3]/text()')
            if experience:
                job_data["experience_required"] = experience[0].replace('|', '').strip()
            else:
                job_data["experience_required"] = None

            visa_block = tree.xpath('//span[contains(@class, "text-md font-semibold") and text()="Visa Sponsorship"]/following-sibling::p[1]/span/text()')
            job_data["visa"] = visa_block[0].strip() if visa_block else None

            remote_block = tree.xpath('//span[contains(@class, "text-md font-semibold") and text()="Remote Work Policy"]/following-sibling::p[1]/text()')
            job_data["remote_work_pol"] = remote_block[0].strip() if remote_block else None

            relocation_block = tree.xpath('//span[contains(@class, "text-md font-semibold") and text()="Relocation"]/following-sibling::span[@class="flex items-center"]/text()')
            job_data["relocation"] = relocation_block[0].strip() if relocation_block else None

            skills_divs = tree.xpath('//span[contains(@class, "text-md font-semibold") and text()="Skills"]/following-sibling::div[@class="flex flex-wrap"]/div/text()')
            job_data["skills"] = [skill.strip() for skill in skills_divs if skill.strip()]

            desc_div = tree.xpath('//div[@id="job-description"]')
            if desc_div:
                job_data["job_description"] = ' '.join(desc_div[0].xpath('.//text()')).strip()
            else:
                job_data["job_description"] = None

            div_a_selector = "div.grid.grid-cols-2.items-center.gap-2.border-t.border-gray-300.p-4.xs\\:flex.xs\\:flex-wrap"
            div_a_elements = tree.cssselect(div_a_selector)

            for div_a in div_a_elements:
                div_b_selector = "div.flex.items-center.justify-center.rounded.bg-gray-200.px-2.font-medium"
                div_b_elements = div_a.cssselect(div_b_selector)

                for div_b in div_b_elements:
                    img_elements = div_b.cssselect("img")
                    if not img_elements:
                        continue  # Skip if no img tag is found
                    key = img_elements[0].get("alt")

                    inner_div_selector = (
                        "div.flex.h-\\[20px\\].items-center.justify-center.space-x-1.whitespace-nowrap.pl-1."
                        "text-\\[10px\\].font-medium.leading-relaxed.text-accent-persian-600"
                    )
                    inner_div_elements = div_b.cssselect(inner_div_selector)

                    if inner_div_elements:
                        value = inner_div_elements[0].text_content().strip()
                        if key not in job_data:
                            job_data[key] = []
                        job_data[key].append(value)

            div_c_selector = "div.flex.w-full.space-x-2.text-\\[10px\\].text-accent-persian-600.border-t.border-gray-300.p-4.pb-4"
            div_c_elements = tree.cssselect(div_c_selector)

            result_array = []
            if div_c_elements:
                ul_elements = div_c_elements[0].cssselect("ul")
                if ul_elements:
                    line_clamp_divs = ul_elements[0].cssselect("div.line-clamp-1")
                    for div in line_clamp_divs:
                        text = div.text_content().strip()
                        result_array.append(text)

            job_data["additional_data"] = result_array

            div_a_selector = "div.grid.w-full.grid-cols-1.gap-4.sm\\:grid-cols-3.max-xs\\:grid-cols-2"
            div_a_elements = tree.cssselect(div_a_selector)

            results = []

            for div_a in div_a_elements:
                div_b_elements = div_a.cssselect("div.mt-4.flex.items-start")
                for div_b in div_b_elements:
                    text = div_b.cssselect("div.h-full.pl-2")[0].text_content().strip()
                    results.append(text)

            job_data["perks"] = results

            div_a = tree.cssselect('div.mt-4.flex.rounded-lg.bg-brand-burgandy.p-4.text-center')[0]
            div_b_list = div_a.cssselect('div.w-1\\/2.border-r.border-white')

            for div_b in div_b_list:
                key_element = div_b.cssselect('div.text-xs.text-gray-400')[0]
                key = key_element.text_content().strip()

                value_element = div_b.cssselect('div.text-m.font-semibold.text-white')[0]
                value = value_element.text_content().strip()

                job_data[key] = value

            span_selector = "span.text-lg.font-semibold.text-black"
            span_elements = tree.cssselect(span_selector)
            span_text = span_elements[0].text_content().strip() if span_elements else None

            job_data["founder"] = span_text
            final_data.append(job_data)

        except Exception as e:
            print("Skipping this page due to parse error:", e)

    return final_data

if __name__ == "__main__":
    # — Connect to MongoDB —
    client = MongoClient("mongodb://localhost:27017/")
    db = client["job_scraping"]
    urls_col = db["job_urls"]
    jobs_col = db["jobs"]

    # Fetch all URLs not yet scraped
    pending = list(urls_col.find({"scraped": False}))
    if not pending:
        print("✅ No URLs to scrape.")
        exit()

    for doc in pending:
        url = doc["url"]
        print(f"\n🔗 Scraping URL: {url}")

        success = False
        for attempt in range(1, 4):
            try:
                data = get_job_desc([url])
                if data:
                    jobs_col.insert_many(data)
                    print(f"✔ Inserted {len(data)} records for {url}")
                else:
                    print(f"⚠ No data extracted from {url}")
                # Mark as scraped regardless of data/no-data
                urls_col.update_one({"_id": doc["_id"]}, {"$set": {"scraped": True}})
                success = True
                break
            except Exception as e:
                print(f"❌ Attempt {attempt} failed for {url}: {e}")
                if attempt < 3:
                    print("   Retrying in 5s...")
                    time.sleep(5)
                else:
                    print(f"✖ Failed to scrape {url} after 3 attempts.")
        if not success:
            # Optionally, you can choose to mark it scraped to avoid endless loops:
            # urls_col.update_one({"_id": doc["_id"]}, {"$set": {"scraped": True}})
            pass

        # be polite between URLs
        time.sleep(2)
