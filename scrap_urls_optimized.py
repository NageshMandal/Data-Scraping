#!/usr/bin/env python3
"""
Optimized URL Scraper with Concurrency
=====================================

High-performance URL scraping using threading and smart rate limiting.
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
from threading import Lock, BoundedSemaphore
from collections import defaultdict
import threading
from queue import Queue
import signal
import sys

# Load environment variables
load_dotenv()

# ——— Performance Configuration for M1 Pro + ZenRows Startup Plan ———
MAX_WORKERS = 20  # Take advantage of 50 concurrent request limit
RATE_LIMIT_PER_SECOND = 15  # Much higher rate with startup plan
BATCH_SIZE = 100  # Larger batches with higher limits
MAX_RETRIES = 3
BASE_DELAY = 0.5  # Reduced delay with higher limits

# ——— Global Rate Limiter ———
class RateLimiter:
    def __init__(self, max_calls_per_second):
        self.max_calls = max_calls_per_second
        self.calls = []
        self.lock = Lock()
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            # Remove calls older than 1 second
            self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            if len(self.calls) >= self.max_calls:
                sleep_time = 1.0 - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            self.calls.append(now)

# Global rate limiter instance
rate_limiter = RateLimiter(RATE_LIMIT_PER_SECOND)

# ——— ZenRows Configuration ———
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")
if not ZENROWS_API_KEY:
    raise RuntimeError("ZENROWS_API_KEY not found in .env file")

ZENROWS_API_URL = "https://api.zenrows.com/v1/"

# ——— Statistics Tracking ———
class StatsTracker:
    def __init__(self):
        self.lock = Lock()
        self.total_processed = 0
        self.total_success = 0
        self.total_errors = 0
        self.total_jobs_found = 0
        self.start_time = time.time()
        
    def increment_processed(self):
        with self.lock:
            self.total_processed += 1
            
    def increment_success(self, jobs_count=0):
        with self.lock:
            self.total_success += 1
            self.total_jobs_found += jobs_count
            
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
                'jobs_found': self.total_jobs_found,
                'elapsed': elapsed,
                'rate': self.total_processed / elapsed if elapsed > 0 else 0
            }

stats = StatsTracker()

def optimized_zenrows_request(url, timeout=60):
    """
    Optimized ZenRows request with rate limiting and smart retries
    """
    rate_limiter.wait_if_needed()
    
    params = {
        'apikey': ZENROWS_API_KEY,
        'url': url,
        'js_render': 'true',
        'premium_proxy': 'true',
        'wait': '2000',  # Reduced wait time
        'wait_for': '.text-brand-burgandy',
        'block_resources': 'image,stylesheet,font,media'
    }
    
    try:
        response = requests.get(ZENROWS_API_URL, params=params, timeout=timeout)
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 429:
            # Rate limit hit - exponential backoff
            time.sleep(random.uniform(5, 10))
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

def get_mongo_connection():
    """Get thread-local MongoDB connection"""
    if not hasattr(thread_local, 'client'):
        thread_local.client = MongoClient(MONGO_URI)
        thread_local.db = thread_local.client[DB_NAME]
        thread_local.collection = thread_local.db["job_urls"]
        thread_local.collection.create_index("url", unique=True)
    return thread_local.collection

# ——— Batch URL processor ———
class BatchURLProcessor:
    def __init__(self, batch_size=BATCH_SIZE):
        self.batch_size = batch_size
        self.batch = []
        self.lock = Lock()
        
    def add_url(self, url):
        with self.lock:
            self.batch.append({
                "url": url,
                "processed": False,
                "scraped": False
            })
            
            if len(self.batch) >= self.batch_size:
                self._flush_batch()
                
    def _flush_batch(self):
        if not self.batch:
            return
            
        collection = get_mongo_connection()
        operations = []
        
        for doc in self.batch:
            operations.append({
                "updateOne": {
                    "filter": {"url": doc["url"]},
                    "update": {"$setOnInsert": doc},
                    "upsert": True
                }
            })
        
        try:
            if operations:
                collection.bulk_write(operations, ordered=False)
        except Exception as e:
            print(f"⚠️ Batch write error: {e}")
            
        self.batch.clear()
        
    def flush(self):
        with self.lock:
            self._flush_batch()

batch_processor = BatchURLProcessor()

def scrape_single_page(url, page=1):
    """
    Scrape a single page and extract job URLs
    Returns: (success, job_urls_found, error_message)
    """
    page_url = url if page == 1 else f"{url}?page={page}"
    
    for attempt in range(MAX_RETRIES):
        html_src = optimized_zenrows_request(page_url)
        
        if html_src is None:
            if attempt < MAX_RETRIES - 1:
                time.sleep(random.uniform(1, 3))
            continue
            
        # Check for common failure cases
        if "Page not found (404)" in html_src:
            return False, 0, "404 Not Found"
            
        if "0 results total" in html_src:
            return False, 0, "No results"
            
        try:
            tree = html.fromstring(html_src)
            hrefs = tree.xpath(
                '//a[contains(@class, "mr-2") and contains(@class, "text-brand-burgandy")]/@href'
            )
            
            if not hrefs:
                if attempt < MAX_RETRIES - 1:
                    continue
                return False, 0, "No job links found"
                
            # Process found URLs
            job_urls = []
            for href in hrefs:
                full_url = (url.split("/role")[0] + href) if href.startswith("/") else href
                job_urls.append(full_url)
                batch_processor.add_url(full_url)
                
            return True, len(job_urls), None
            
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(random.uniform(1, 2))
                continue
            return False, 0, f"Parse error: {e}"
    
    return False, 0, "Max retries exceeded"

def scrape_url_worker(url_entry):
    """
    Worker function to scrape all pages for a single base URL
    """
    base_url = url_entry["url"]
    thread_id = threading.current_thread().ident
    
    try:
        total_jobs = 0
        page = 1
        consecutive_failures = 0
        
        while consecutive_failures < 3:  # Stop after 3 consecutive page failures
            success, jobs_count, error = scrape_single_page(base_url, page)
            
            if success:
                total_jobs += jobs_count
                consecutive_failures = 0
                print(f"🔧 Thread-{thread_id % 1000:03d} | Page {page} | {base_url.split('/')[-2:]}: {jobs_count} jobs")
                
                # Smart pagination - if we found very few jobs, likely near the end
                if jobs_count < 5:
                    break
                    
                page += 1
                time.sleep(random.uniform(0.5, 1.5))  # Reduced delay for pagination
                
            else:
                consecutive_failures += 1
                if error == "No results" or error == "404 Not Found":
                    break  # These are terminal errors for this URL
                    
                if consecutive_failures < 3:
                    time.sleep(random.uniform(2, 4))
        
        stats.increment_success(total_jobs)
        return True, total_jobs
        
    except Exception as e:
        stats.increment_error()
        print(f"❌ Worker error for {base_url}: {e}")
        return False, 0

def progress_reporter():
    """Background thread to report progress"""
    while True:
        time.sleep(30)  # Report every 30 seconds
        stats_data = stats.get_stats()
        print(f"\n📊 Progress Report:")
        print(f"   Processed: {stats_data['processed']}/11600")
        print(f"   Success: {stats_data['success']} | Errors: {stats_data['errors']}")
        print(f"   Jobs found: {stats_data['jobs_found']:,}")
        print(f"   Rate: {stats_data['rate']:.1f} URLs/sec")
        print(f"   Elapsed: {stats_data['elapsed']/60:.1f} minutes\n")

# ——— Main optimized processing ———
def main():
    URLS_FILE = os.path.join(os.path.dirname(__file__), "wellfound_urls.json")
    
    if not os.path.exists(URLS_FILE):
        print(f"❌ URLs file not found: {URLS_FILE}")
        print("Please run: python main.py --step generate_urls first")
        return
    
    # Load URLs
    with open(URLS_FILE, "r") as f:
        targets = json.load(f)
    
    # Filter unprocessed URLs
    unprocessed = [entry for entry in targets if not entry.get("value", False)]
    
    print(f"🚀 Optimized URL Scraper Starting")
    print(f"📊 Total URLs: {len(targets)}")
    print(f"📋 Unprocessed: {len(unprocessed)}")
    print(f"🔧 Max Workers: {MAX_WORKERS}")
    print(f"⚡ Rate Limit: {RATE_LIMIT_PER_SECOND} req/sec")
    print("=" * 50)
    
    if not unprocessed:
        print("✅ All URLs already processed!")
        return
    
    # Start progress reporter in background
    progress_thread = threading.Thread(target=progress_reporter, daemon=True)
    progress_thread.start()
    
    # Process URLs concurrently
    completed_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all unprocessed URLs
        future_to_url = {
            executor.submit(scrape_url_worker, url_entry): url_entry 
            for url_entry in unprocessed
        }
        
        try:
            for future in as_completed(future_to_url):
                url_entry = future_to_url[future]
                stats.increment_processed()
                
                try:
                    success, jobs_found = future.result()
                    if success:
                        # Mark as processed in the original data
                        url_entry["value"] = True
                        completed_count += 1
                        
                        # Periodically save progress
                        if completed_count % 100 == 0:
                            with open(URLS_FILE, "w") as f:
                                json.dump(targets, f, indent=2)
                            print(f"💾 Progress saved: {completed_count}/{len(unprocessed)}")
                            
                except Exception as e:
                    print(f"❌ Future error: {e}")
                    stats.increment_error()
        
        except KeyboardInterrupt:
            print("\n🛑 Interrupted by user")
            executor.shutdown(wait=False)
    
    # Final cleanup
    batch_processor.flush()
    
    # Save final results
    with open(URLS_FILE, "w") as f:
        json.dump(targets, f, indent=2)
    
    # Final statistics
    stats_data = stats.get_stats()
    collection = get_mongo_connection()
    total_urls = collection.count_documents({})
    unscraped = collection.count_documents({"scraped": False})
    
    print(f"\n" + "=" * 50)
    print(f"🎉 Scraping Complete!")
    print(f"📊 Final Statistics:")
    print(f"   URLs processed: {stats_data['processed']}")
    print(f"   Successful: {stats_data['success']}")
    print(f"   Errors: {stats_data['errors']}")
    print(f"   Job URLs found: {stats_data['jobs_found']:,}")
    print(f"   Total time: {stats_data['elapsed']/60:.1f} minutes")
    print(f"   Average rate: {stats_data['rate']:.1f} URLs/sec")
    print(f"\n📈 Database Stats:")
    print(f"   Total URLs in MongoDB: {total_urls:,}")
    print(f"   Ready for job scraping: {unscraped:,}")
    print(f"🚀 Next step: python main.py --step scrape_jobs")

if __name__ == "__main__":
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print('\n🛑 Graceful shutdown...')
        batch_processor.flush()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    main() 