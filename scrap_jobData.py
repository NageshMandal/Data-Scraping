import os
import random
import json
import time
import requests
from lxml import html
from pymongo import MongoClient
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
        'wait': '5000',  # Wait 5 seconds for job page to load
        'wait_for': '[data-test="JobListing"]',  # Wait for job content to appear
    }
    
    # Add block_resources only if specified (ZenRows uses specific format)
    if block_resources:
        params['block_resources'] = block_resources
    
    print(f"🔄 ZenRows request: {url}")
    
    try:
        response = requests.get(ZENROWS_API_URL, params=params, timeout=120)
        
        if response.status_code == 200:
            print(f"✅ Success - Response length: {len(response.text)}")
            return response.text
        elif response.status_code == 422:
            print("⚠️ ZenRows: Unprocessable request - might be blocked or invalid URL")
            return None
        elif response.status_code == 429:
            print("⚠️ ZenRows: Rate limit hit - waiting...")
            time.sleep(15)
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

def extract_job_data(html_content):
    """
    Extract job data from HTML content
    """
    try:
        tree = html.fromstring(html_content)
        job_data = {}
        
        # Find the main job listing div
        job_div = tree.xpath('//div[@data-test="JobListing"]')
        if not job_div:
            print("⚠️ No JobListing div found")
            return None
        
        job_div = job_div[0]

        # Company name
        company = job_div.xpath('.//span[contains(@class, "text-sm font-semibold text-black")]/text()')
        job_data["company_name"] = company[0].strip() if company else None

        print(f"🏢 Company: {job_data['company_name']}")

        # Hiring status
        hiring_status = job_div.xpath(".//div[contains(@class, 'flex items-center text-sm font-medium text-pop-green')]/text()")
        job_data["hiring_stat"] = hiring_status

        # Company slogan
        slogan = job_div.xpath(".//div[contains(@class, 'text-sm font-light text-neutral-500')]/text()")
        job_data["slogan"] = slogan

        # Job position/title
        position = job_div.xpath('.//h1[contains(@class, "inline text-xl font-semibold text-black")]/text()')
        job_data["position"] = position[0].strip() if position else None

        # Job details (salary, location, experience)
        ul = job_div.xpath('.//ul[contains(@class, "block text-md text-black md:flex")]')
        if ul:
            ul = ul[0]
            
            # Salary
            price = ul.xpath('./li[1]/text()')
            job_data["price"] = price[0].strip() if price else None

            # Location
            location = ul.xpath('./li[2]//a[contains(@class, "font-normal text-black text-md")]/text()')
            job_data["location"] = location[0].strip() if location else None

            # Experience level
            experience = ul.xpath('./li[3]/text()')
            if experience:
                job_data["experience_required"] = experience[0].replace('|', '').strip()
            else:
                job_data["experience_required"] = None

        # Visa sponsorship
        visa_block = tree.xpath('//span[contains(@class, "text-md font-semibold") and text()="Visa Sponsorship"]/following-sibling::p[1]/span/text()')
        job_data["visa"] = visa_block[0].strip() if visa_block else None

        # Remote work policy
        remote_block = tree.xpath('//span[contains(@class, "text-md font-semibold") and text()="Remote Work Policy"]/following-sibling::p[1]/text()')
        job_data["remote_work_pol"] = remote_block[0].strip() if remote_block else None

        # Relocation
        relocation_block = tree.xpath('//span[contains(@class, "text-md font-semibold") and text()="Relocation"]/following-sibling::span[@class="flex items-center"]/text()')
        job_data["relocation"] = relocation_block[0].strip() if relocation_block else None

        # Skills
        skills_divs = tree.xpath('//span[contains(@class, "text-md font-semibold") and text()="Skills"]/following-sibling::div[@class="flex flex-wrap"]/div/text()')
        job_data["skills"] = [skill.strip() for skill in skills_divs if skill.strip()]

        # Job description
        desc_div = tree.xpath('//div[@id="job-description"]')
        if desc_div:
            job_data["job_description"] = ' '.join(desc_div[0].xpath('.//text()')).strip()
        else:
            job_data["job_description"] = None

        # Company information grid
        div_a_selector = "div.grid.grid-cols-2.items-center.gap-2.border-t.border-gray-300.p-4.xs\\:flex.xs\\:flex-wrap"
        div_a_elements = tree.cssselect(div_a_selector)

        for div_a in div_a_elements:
            div_b_selector = "div.flex.items-center.justify-center.rounded.bg-gray-200.px-2.font-medium"
            div_b_elements = div_a.cssselect(div_b_selector)

            for div_b in div_b_elements:
                img_elements = div_b.cssselect("img")
                if not img_elements:
                    continue
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

        # Additional company data
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

        # Company perks
        div_a_selector = "div.grid.w-full.grid-cols-1.gap-4.sm\\:grid-cols-3.max-xs\\:grid-cols-2"
        div_a_elements = tree.cssselect(div_a_selector)

        results = []
        for div_a in div_a_elements:
            div_b_elements = div_a.cssselect("div.mt-4.flex.items-start")
            for div_b in div_b_elements:
                text_elements = div_b.cssselect("div.h-full.pl-2")
                if text_elements:
                    text = text_elements[0].text_content().strip()
                    results.append(text)

        job_data["perks"] = results

        # Company funding info
        try:
            div_a = tree.cssselect('div.mt-4.flex.rounded-lg.bg-brand-burgandy.p-4.text-center')
            if div_a:
                div_a = div_a[0]
                div_b_list = div_a.cssselect('div.w-1\\/2.border-r.border-white')

                for div_b in div_b_list:
                    key_elements = div_b.cssselect('div.text-xs.text-gray-400')
                    value_elements = div_b.cssselect('div.text-m.font-semibold.text-white')
                    
                    if key_elements and value_elements:
                        key = key_elements[0].text_content().strip()
                        value = value_elements[0].text_content().strip()
                        job_data[key] = value
        except Exception as e:
            print(f"⚠️ Error extracting funding info: {e}")

        # Founder info
        span_selector = "span.text-lg.font-semibold.text-black"
        span_elements = tree.cssselect(span_selector)
        span_text = span_elements[0].text_content().strip() if span_elements else None
        job_data["founder"] = span_text

        return job_data

    except Exception as e:
        print(f"❌ Error extracting job data: {e}")
        return None

def scrape_job_data(url):
    """
    Scrape job data from a single URL using ZenRows
    """
    for attempt in range(1, 4):
        print(f"  Attempt {attempt}/3")
        
        html_content = zenrows_request(url)
        
        if html_content is None:
            print(f"  ⚠️ ZenRows request failed on attempt {attempt}")
            if attempt < 3:
                sleep_time = random.uniform(5, 10)
                print(f"  😴 Waiting {sleep_time:.1f}s before retry...")
                time.sleep(sleep_time)
            continue
        
        # Extract job data from HTML
        job_data = extract_job_data(html_content)
        
        if job_data:
            return job_data
        else:
            print(f"  ⚠️ Failed to extract job data on attempt {attempt}")
            if attempt < 3:
                time.sleep(random.uniform(3, 7))
    
    print("  ❌ Failed to scrape job data after 3 attempts")
    return None

if __name__ == "__main__":
    print("🚀 Starting ZenRows-powered job data scraping...")
    
    # — Connect to MongoDB —
    MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("❌ ERROR: MONGO_URI not found in environment variables!")
    exit(1)
    DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")
    
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    urls_col = db["job_urls"]
    jobs_col = db["jobs"]

    # Fetch all URLs not yet scraped
    pending = list(urls_col.find({"scraped": False}))
    total_pending = len(pending)
    
    if not pending:
        print("✅ No URLs to scrape.")
        exit()

    print(f"📋 Found {total_pending} URLs to scrape")

    success_count = 0
    error_count = 0

    for i, doc in enumerate(pending, 1):
        url = doc["url"]
        print(f"\n🔗 [{i}/{total_pending}] Scraping: {url}")

        job_data = scrape_job_data(url)
        
        if job_data:
            try:
                # Add metadata
                job_data["scraped_at"] = time.time()
                job_data["source_url"] = url
                
                # Insert job data
                result = jobs_col.insert_one(job_data)
                print(f"  ✅ Job data saved to MongoDB (ID: {result.inserted_id})")
                
                # Mark URL as scraped
                urls_col.update_one(
                    {"_id": doc["_id"]}, 
                    {"$set": {"scraped": True, "scraped_at": time.time()}}
                )
                
                success_count += 1
                
            except Exception as e:
                print(f"  ❌ MongoDB error: {e}")
                error_count += 1
        else:
            print(f"  ❌ Failed to extract job data")
            # Still mark as scraped to avoid infinite loops
            urls_col.update_one(
                {"_id": doc["_id"]}, 
                {"$set": {"scraped": True, "scraped_at": time.time(), "scrape_failed": True}}
            )
            error_count += 1

        # Be polite between requests
        if i < total_pending:
            sleep_time = random.uniform(3, 8)
            print(f"  😴 Sleeping {sleep_time:.1f}s before next job...")
            time.sleep(sleep_time)

    print(f"\n🎉 Job scraping completed!")
    print(f"✅ Successfully scraped: {success_count}")
    print(f"❌ Failed: {error_count}")
    print(f"📊 Total processed: {success_count + error_count}")
    print("🚀 Next step: python main.py --step classify")
