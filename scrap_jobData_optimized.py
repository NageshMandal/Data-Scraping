#!/usr/bin/env python3
"""
Optimized Job Data Scraper with Concurrency
==========================================

High-performance job data extraction using threading and batch processing.
Optimized for MacBook M1 Pro with careful API limit management.
"""

import os
import json
import random
import time
import requests
from lxml import html
from pymongo import MongoClient, errors
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import threading
import signal
import sys

# Load environment variables
load_dotenv()

# ——— Performance Configuration for M1 Pro + ZenRows Startup Plan ———
MAX_WORKERS = 15  # Higher concurrency for job data scraping
RATE_LIMIT_PER_SECOND = 12  # Higher rate for detailed scraping
BATCH_SIZE = 50  # Larger batches for job data
MAX_RETRIES = 3
TIMEOUT = 90

# ——— Global Rate Limiter (reused from URL scraper) ———
class RateLimiter:
    def __init__(self, max_calls_per_second):
        self.max_calls = max_calls_per_second
        self.calls = []
        self.lock = Lock()
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            if len(self.calls) >= self.max_calls:
                sleep_time = 1.0 - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            self.calls.append(now)

rate_limiter = RateLimiter(RATE_LIMIT_PER_SECOND)

# ——— ZenRows Configuration ———
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")
if not ZENROWS_API_KEY:
    raise RuntimeError("ZENROWS_API_KEY not found in .env file")

ZENROWS_API_URL = "https://api.zenrows.com/v1/"

# ——— Statistics Tracking ———
class JobStatsTracker:
    def __init__(self):
        self.lock = Lock()
        self.total_processed = 0
        self.total_success = 0
        self.total_errors = 0
        self.start_time = time.time()
        
    def increment_processed(self):
        with self.lock:
            self.total_processed += 1
            
    def increment_success(self):
        with self.lock:
            self.total_success += 1
            
    def increment_error(self):
        with self.lock:
            self.total_errors += 1
            
    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            return {
                'processed': self.total_processed,
                'success': self.total_success,
                'errors': self.total_errors,
                'elapsed': elapsed,
                'rate': self.total_processed / elapsed if elapsed > 0 else 0
            }

stats = JobStatsTracker()

def optimized_zenrows_request(url):
    """Optimized ZenRows request for job data scraping"""
    rate_limiter.wait_if_needed()
    
    params = {
        'apikey': ZENROWS_API_KEY,
        'url': url,
        'js_render': 'true',
        'premium_proxy': 'true',
        'wait': '3000',  # Longer wait for job pages
        'block_resources': 'image,stylesheet,font'
    }
    
    try:
        response = requests.get(ZENROWS_API_URL, params=params, timeout=TIMEOUT)
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 429:
            time.sleep(random.uniform(8, 15))
            return None
        else:
            return None
            
    except Exception:
        return None

# ——— MongoDB setup with connection pooling ———
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("❌ ERROR: MONGO_URI not found in environment variables!")
    exit(1)
DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")

# Thread-local MongoDB connections
thread_local = threading.local()

def get_mongo_connections():
    """Get thread-local MongoDB connections"""
    if not hasattr(thread_local, 'client'):
        thread_local.client = MongoClient(MONGO_URI)
        thread_local.db = thread_local.client[DB_NAME]
        thread_local.job_urls = thread_local.db["job_urls"]
        thread_local.job_data = thread_local.db["job_data"]
        
        # Create indexes
        thread_local.job_urls.create_index("url", unique=True)
        thread_local.job_data.create_index("url", unique=True)
        
    return thread_local.job_urls, thread_local.job_data

# ——— Job Data Extractor ———
def extract_job_data(html_content, url):
    """Extract comprehensive job data from HTML"""
    try:
        tree = html.fromstring(html_content)
        
        # Initialize job data structure
        job_data = {
            "url": url,
            "scraped_at": time.time()
        }
        
        # Helper function to safely extract text
        def safe_extract(xpath, default=""):
            try:
                elements = tree.xpath(xpath)
                return elements[0].strip() if elements else default
            except:
                return default
        
        def safe_extract_list(xpath):
            try:
                return [elem.strip() for elem in tree.xpath(xpath) if elem.strip()]
            except:
                return []
        
        # Company information
        job_data["company_name"] = safe_extract('//h1[@class="text-2xl font-medium"]/text()')
        job_data["company_location"] = safe_extract('//div[contains(@class, "text-neutral-500")]//span[contains(text(), "📍")]/following-sibling::text()[1]')
        job_data["company_size"] = safe_extract('//div[contains(text(), "employees")]/text()')
        job_data["company_industries"] = safe_extract_list('//div[contains(@class, "tag") and contains(@class, "industry")]//text()')
        job_data["amount_raised"] = safe_extract('//div[contains(text(), "raised")]/text()')
        job_data["founder"] = safe_extract('//div[contains(@class, "founder")]//text()')
        
        # Job specifics
        job_data["position"] = safe_extract('//h1[contains(@class, "job-title")]/text() | //h1[@class="text-2xl"]/text()')
        job_data["location"] = safe_extract('//div[contains(@class, "location")]//text() | //span[contains(text(), "📍")]/following-sibling::text()[1]')
        job_data["job_description"] = safe_extract('//div[contains(@class, "job-description")]//text() | //div[@class="prose"]//text()')
        job_data["price"] = safe_extract('//div[contains(@class, "salary") or contains(@class, "compensation")]//text()')
        
        # Skills and requirements
        job_data["skills"] = safe_extract_list('//div[contains(@class, "skill") or contains(@class, "tag")]//text()')
        job_data["visa"] = safe_extract('//div[contains(text(), "visa") or contains(text(), "Visa")]//text()')
        job_data["remote_work_pol"] = safe_extract('//div[contains(text(), "remote") or contains(text(), "Remote")]//text()')
        
        # Additional company data
        job_data["hiring_stat"] = safe_extract('//div[contains(@class, "hiring")]//text()')
        job_data["year_founded"] = safe_extract('//div[contains(text(), "Founded")]//text()')
        
        # Clean up empty fields
        job_data = {k: v for k, v in job_data.items() if v}
        
        return job_data
        
    except Exception as e:
        return {"url": url, "error": f"Extraction failed: {e}", "scraped_at": time.time()}

def scrape_job_worker(job_url_doc):
    """Worker function to scrape a single job posting"""
    url = job_url_doc["url"]
    thread_id = threading.current_thread().ident
    
    try:
        # Get MongoDB connections for this thread
        job_urls_col, job_data_col = get_mongo_connections()
        
        # Check if already scraped
        existing = job_data_col.find_one({"url": url})
        if existing:
            return True, "Already scraped"
        
        # Scrape the job page
        for attempt in range(MAX_RETRIES):
            html_content = optimized_zenrows_request(url)
            
            if html_content is None:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(random.uniform(2, 5))
                continue
            
            # Check for error pages
            if "Page not found" in html_content or "404" in html_content:
                job_data = {"url": url, "error": "404 Not Found", "scraped_at": time.time()}
                break
            
            # Extract job data
            job_data = extract_job_data(html_content, url)
            
            if job_data and len(job_data) > 3:  # More than just url, error, scraped_at
                break
            elif attempt < MAX_RETRIES - 1:
                time.sleep(random.uniform(1, 3))
        else:
            job_data = {"url": url, "error": "Max retries exceeded", "scraped_at": time.time()}
        
        # Save job data
        try:
            job_data_col.update_one(
                {"url": url},
                {"$set": job_data},
                upsert=True
            )
            
            # Mark as scraped in job_urls collection
            job_urls_col.update_one(
                {"url": url},
                {"$set": {"scraped": True, "scraped_at": time.time()}}
            )
            
            # Quick success indicator
            success = "error" not in job_data
            company = job_data.get("company_name", "Unknown")
            position = job_data.get("position", "Unknown")
            
            if success:
                print(f"✅ Thread-{thread_id % 1000:03d} | {company} - {position}")
            else:
                print(f"⚠️ Thread-{thread_id % 1000:03d} | {url} - {job_data.get('error', 'Unknown error')}")
            
            return success, job_data.get('error', 'Success')
            
        except Exception as e:
            return False, f"Database error: {e}"
            
    except Exception as e:
        return False, f"Worker error: {e}"

def progress_reporter():
    """Background thread to report progress"""
    while True:
        time.sleep(45)  # Report every 45 seconds
        stats_data = stats.get_stats()
        
        # Get current database counts
        try:
            job_urls_col, job_data_col = get_mongo_connections()
            total_urls = job_urls_col.count_documents({})
            scraped_count = job_data_col.count_documents({})
            remaining = job_urls_col.count_documents({"scraped": {"$ne": True}})
        except:
            total_urls = scraped_count = remaining = 0
        
        print(f"\n📊 Job Scraping Progress:")
        print(f"   Processed: {stats_data['processed']}")
        print(f"   Success: {stats_data['success']} | Errors: {stats_data['errors']}")
        print(f"   Database: {scraped_count:,} scraped / {total_urls:,} total")
        print(f"   Remaining: {remaining:,}")
        print(f"   Rate: {stats_data['rate']:.1f} jobs/sec")
        print(f"   Elapsed: {stats_data['elapsed']/60:.1f} minutes\n")

# ——— Main optimized processing ———
def main():
    print(f"🚀 Optimized Job Data Scraper Starting")
    print(f"🔧 Max Workers: {MAX_WORKERS}")
    print(f"⚡ Rate Limit: {RATE_LIMIT_PER_SECOND} req/sec")
    print("=" * 50)
    
    # Get MongoDB connections
    job_urls_col, job_data_col = get_mongo_connections()
    
    # Get unscraped job URLs
    unscraped_cursor = job_urls_col.find({"scraped": {"$ne": True}})
    unscraped_urls = list(unscraped_cursor)
    
    total_urls = job_urls_col.count_documents({})
    print(f"📊 Total job URLs: {total_urls:,}")
    print(f"📋 Unscraped: {len(unscraped_urls):,}")
    
    if not unscraped_urls:
        print("✅ All job URLs already scraped!")
        return
    
    # Start progress reporter
    progress_thread = threading.Thread(target=progress_reporter, daemon=True)
    progress_thread.start()
    
    # Process jobs concurrently
    completed_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit jobs in batches to manage memory
        batch_size = 500
        
        for i in range(0, len(unscraped_urls), batch_size):
            batch = unscraped_urls[i:i + batch_size]
            print(f"🔄 Processing batch {i//batch_size + 1}: {len(batch)} jobs")
            
            # Submit batch
            future_to_url = {
                executor.submit(scrape_job_worker, job_doc): job_doc
                for job_doc in batch
            }
            
            try:
                for future in as_completed(future_to_url):
                    job_doc = future_to_url[future]
                    stats.increment_processed()
                    
                    try:
                        success, message = future.result()
                        if success:
                            stats.increment_success()
                        else:
                            stats.increment_error()
                            
                        completed_count += 1
                        
                        # Progress indicator
                        if completed_count % 100 == 0:
                            print(f"📈 Progress: {completed_count}/{len(unscraped_urls)}")
                            
                    except Exception as e:
                        stats.increment_error()
                        print(f"❌ Future error: {e}")
            
            except KeyboardInterrupt:
                print("\n🛑 Interrupted by user")
                executor.shutdown(wait=False)
                break
    
    # Final statistics
    stats_data = stats.get_stats()
    final_scraped = job_data_col.count_documents({})
    final_remaining = job_urls_col.count_documents({"scraped": {"$ne": True}})
    
    print(f"\n" + "=" * 50)
    print(f"🎉 Job Data Scraping Complete!")
    print(f"📊 Final Statistics:")
    print(f"   Jobs processed: {stats_data['processed']:,}")
    print(f"   Successful: {stats_data['success']:,}")
    print(f"   Errors: {stats_data['errors']:,}")
    print(f"   Total time: {stats_data['elapsed']/60:.1f} minutes")
    print(f"   Average rate: {stats_data['rate']:.1f} jobs/sec")
    print(f"\n📈 Database Stats:")
    print(f"   Jobs scraped: {final_scraped:,}")
    print(f"   Remaining: {final_remaining:,}")
    print(f"🚀 Next step: python main.py --step classify_data")

if __name__ == "__main__":
    def signal_handler(sig, frame):
        print('\n🛑 Graceful shutdown...')
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    main() 