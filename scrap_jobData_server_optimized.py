#!/usr/bin/env python3
"""
Server-Optimized Job Data Scraper
================================

Ultra high-performance job data extraction for 92GB RAM server:
- 35 concurrent workers
- 20 req/sec rate limiting
- 4GB memory buffer
- 150 docs/batch processing
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
import psutil

# Load environment variables
load_dotenv()

# Load server configuration
def load_server_config():
    config_path = os.getenv('SERVER_CONFIG_PATH', 'config/server_config.json')
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            return config["performance_config"]["job_data_scraping"]
    except:
        # Server-optimized defaults
        return {
            "max_workers": 35,
            "rate_limit_per_second": 20,
            "batch_size": 150,
            "max_retries": 3,
            "timeout": 120,
            "memory_buffer_mb": 4096
        }

# ——— Server Performance Configuration ———
server_config = load_server_config()
MAX_WORKERS = server_config["max_workers"]
RATE_LIMIT_PER_SECOND = server_config["rate_limit_per_second"]
BATCH_SIZE = server_config["batch_size"]
MAX_RETRIES = server_config["max_retries"]
TIMEOUT = server_config["timeout"]

print(f"🔥 SERVER-OPTIMIZED JOB DATA SCRAPER")
print(f"   Workers: {MAX_WORKERS} (Server-grade)")
print(f"   Rate Limit: {RATE_LIMIT_PER_SECOND} req/sec")
print(f"   Batch Size: {BATCH_SIZE}")

# ——— Enhanced Rate Limiter ———
class ServerRateLimiter:
    def __init__(self, max_calls_per_second):
        self.max_calls = max_calls_per_second
        self.calls = []
        self.lock = Lock()
        self.adaptive_factor = 1.0
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            # Adaptive rate limiting based on server load
            cpu_usage = psutil.cpu_percent(interval=0.1)
            if cpu_usage > 80:
                self.adaptive_factor = 0.8
            elif cpu_usage < 50:
                self.adaptive_factor = 1.3
            else:
                self.adaptive_factor = 1.0
            
            effective_max_calls = int(self.max_calls * self.adaptive_factor)
            
            if len(self.calls) >= effective_max_calls:
                sleep_time = 1.0 - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            self.calls.append(now)

rate_limiter = ServerRateLimiter(RATE_LIMIT_PER_SECOND)

# ——— ZenRows Configuration ———
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")
if not ZENROWS_API_KEY:
    raise RuntimeError("ZENROWS_API_KEY not found in .env file")

ZENROWS_API_URL = "https://api.zenrows.com/v1/"

# ——— Server Statistics Tracking ———
class ServerJobStatsTracker:
    def __init__(self):
        self.lock = Lock()
        self.total_processed = 0
        self.total_success = 0
        self.total_errors = 0
        self.start_time = time.time()
        self.memory_samples = []
        
    def increment_processed(self):
        with self.lock:
            self.total_processed += 1
            
    def increment_success(self):
        with self.lock:
            self.total_success += 1
            
    def increment_error(self):
        with self.lock:
            self.total_errors += 1
    
    def sample_resources(self):
        with self.lock:
            memory_gb = psutil.virtual_memory().used / (1024**3)
            self.memory_samples.append(memory_gb)
            if len(self.memory_samples) > 100:
                self.memory_samples.pop(0)
            
    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            avg_memory = sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0
            return {
                'processed': self.total_processed,
                'success': self.total_success,
                'errors': self.total_errors,
                'elapsed': elapsed,
                'rate': self.total_processed / elapsed if elapsed > 0 else 0,
                'avg_memory_gb': avg_memory,
                'current_memory_gb': psutil.virtual_memory().used / (1024**3)
            }

stats = ServerJobStatsTracker()

def server_optimized_zenrows_request(url):
    """Server-optimized ZenRows request for job data"""
    rate_limiter.wait_if_needed()
    
    params = {
        'apikey': ZENROWS_API_KEY,
        'url': url,
        'js_render': 'true',
        'premium_proxy': 'true',
        'wait': '3000',
        'block_resources': 'image,stylesheet,font'
    }
    
    try:
        response = requests.get(ZENROWS_API_URL, params=params, timeout=TIMEOUT)
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 429:
            time.sleep(random.uniform(10, 18))
            return None
        else:
            return None
            
    except Exception:
        return None

# ——— MongoDB with server optimization ———
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("❌ ERROR: MONGO_URI not found in environment variables!")
    exit(1)
DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")

thread_local = threading.local()

def get_mongo_connections():
    """Get thread-local MongoDB connections with server optimization"""
    if not hasattr(thread_local, 'client'):
        thread_local.client = MongoClient(
            MONGO_URI,
            maxPoolSize=150,  # Server-grade pool
            minPoolSize=75,
            socketTimeoutMS=180000,
            connectTimeoutMS=40000
        )
        thread_local.db = thread_local.client[DB_NAME]
        thread_local.job_urls = thread_local.db["job_urls"]
        thread_local.job_data = thread_local.db["job_data"]
        
        # Create indexes
        thread_local.job_urls.create_index("url", unique=True)
        thread_local.job_data.create_index("url", unique=True)
        
    return thread_local.job_urls, thread_local.job_data

# ——— Enhanced Job Data Extractor ———
def extract_job_data_server_optimized(html_content, url):
    """Enhanced job data extraction with server optimizations"""
    try:
        tree = html.fromstring(html_content)
        
        job_data = {
            "url": url,
            "scraped_at": time.time(),
            "server_processed": True  # Mark as server-processed
        }
        
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
        
        # Enhanced extraction with more fields
        job_data["company_name"] = safe_extract('//h1[@class="text-2xl font-medium"]/text()')
        job_data["company_location"] = safe_extract('//div[contains(@class, "text-neutral-500")]//span[contains(text(), "📍")]/following-sibling::text()[1]')
        job_data["company_size"] = safe_extract('//div[contains(text(), "employees")]/text()')
        job_data["company_industries"] = safe_extract_list('//div[contains(@class, "tag") and contains(@class, "industry")]//text()')
        job_data["amount_raised"] = safe_extract('//div[contains(text(), "raised")]/text()')
        job_data["founder"] = safe_extract('//div[contains(@class, "founder")]//text()')
        job_data["position"] = safe_extract('//h1[contains(@class, "job-title")]/text() | //h1[@class="text-2xl"]/text()')
        job_data["location"] = safe_extract('//div[contains(@class, "location")]//text() | //span[contains(text(), "📍")]/following-sibling::text()[1]')
        job_data["job_description"] = safe_extract('//div[contains(@class, "job-description")]//text() | //div[@class="prose"]//text()')
        job_data["price"] = safe_extract('//div[contains(@class, "salary") or contains(@class, "compensation")]//text()')
        job_data["skills"] = safe_extract_list('//div[contains(@class, "skill") or contains(@class, "tag")]//text()')
        job_data["visa"] = safe_extract('//div[contains(text(), "visa") or contains(text(), "Visa")]//text()')
        job_data["remote_work_pol"] = safe_extract('//div[contains(text(), "remote") or contains(text(), "Remote")]//text()')
        job_data["hiring_stat"] = safe_extract('//div[contains(@class, "hiring")]//text()')
        job_data["year_founded"] = safe_extract('//div[contains(text(), "Founded")]//text()')
        
        # Clean up empty fields
        job_data = {k: v for k, v in job_data.items() if v}
        
        return job_data
        
    except Exception as e:
        return {"url": url, "error": f"Server extraction failed: {e}", "scraped_at": time.time()}

def scrape_job_worker_server_optimized(job_url_doc):
    """Server-optimized worker function"""
    url = job_url_doc["url"]
    thread_id = threading.current_thread().ident
    
    try:
        job_urls_col, job_data_col = get_mongo_connections()
        
        # Check if already scraped
        existing = job_data_col.find_one({"url": url})
        if existing:
            return True, "Already scraped"
        
        # Scrape with server optimization
        for attempt in range(MAX_RETRIES):
            html_content = server_optimized_zenrows_request(url)
            
            if html_content is None:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(random.uniform(3, 7))
                continue
            
            if "Page not found" in html_content or "404" in html_content:
                job_data = {"url": url, "error": "404 Not Found", "scraped_at": time.time()}
                break
            
            job_data = extract_job_data_server_optimized(html_content, url)
            
            if job_data and len(job_data) > 4:  # More data expected from server processing
                break
            elif attempt < MAX_RETRIES - 1:
                time.sleep(random.uniform(2, 4))
        else:
            job_data = {"url": url, "error": "Max retries exceeded", "scraped_at": time.time()}
        
        # Save with server optimization
        try:
            job_data_col.update_one(
                {"url": url},
                {"$set": job_data},
                upsert=True
            )
            
            job_urls_col.update_one(
                {"url": url},
                {"$set": {"scraped": True, "scraped_at": time.time(), "server_processed": True}}
            )
            
            success = "error" not in job_data
            company = job_data.get("company_name", "Unknown")
            position = job_data.get("position", "Unknown")
            
            if success:
                print(f"✅ Server-{thread_id % 1000:03d} | {company} - {position}")
            else:
                print(f"⚠️ Server-{thread_id % 1000:03d} | {url} - {job_data.get('error', 'Unknown error')}")
            
            return success, job_data.get('error', 'Success')
            
        except Exception as e:
            return False, f"Database error: {e}"
            
    except Exception as e:
        return False, f"Worker error: {e}"

def server_progress_reporter():
    """Enhanced progress reporter with server metrics"""
    while True:
        time.sleep(45)
        stats.sample_resources()
        stats_data = stats.get_stats()
        
        try:
            job_urls_col, job_data_col = get_mongo_connections()
            total_urls = job_urls_col.count_documents({})
            scraped_count = job_data_col.count_documents({})
            remaining = job_urls_col.count_documents({"scraped": {"$ne": True}})
        except:
            total_urls = scraped_count = remaining = 0
        
        print(f"\n📊 SERVER JOB SCRAPING PROGRESS:")
        print(f"   Processed: {stats_data['processed']:,}")
        print(f"   Success: {stats_data['success']:,} | Errors: {stats_data['errors']:,}")
        print(f"   Database: {scraped_count:,} scraped / {total_urls:,} total")
        print(f"   Remaining: {remaining:,}")
        print(f"   Server rate: {stats_data['rate']:.1f} jobs/sec")
        print(f"   Server RAM: {stats_data['current_memory_gb']:.1f}GB used")
        print(f"   Elapsed: {stats_data['elapsed']/60:.1f} minutes\n")

def main():
    print(f"🚀 SERVER-OPTIMIZED JOB DATA SCRAPER STARTING")
    print(f"🔥 MAXIMUM PERFORMANCE MODE")
    print(f"🖥️ Server RAM: {psutil.virtual_memory().total / (1024**3):.1f}GB")
    print("=" * 60)
    
    job_urls_col, job_data_col = get_mongo_connections()
    
    unscraped_cursor = job_urls_col.find({"scraped": {"$ne": True}})
    unscraped_urls = list(unscraped_cursor)
    
    total_urls = job_urls_col.count_documents({})
    print(f"📊 Total job URLs: {total_urls:,}")
    print(f"📋 Unscraped: {len(unscraped_urls):,}")
    
    if not unscraped_urls:
        print("✅ All job URLs already scraped!")
        return
    
    # Start server progress reporter
    progress_thread = threading.Thread(target=server_progress_reporter, daemon=True)
    progress_thread.start()
    
    completed_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Process in server-optimized batches
        batch_size = 750  # Larger batches for server
        
        for i in range(0, len(unscraped_urls), batch_size):
            batch = unscraped_urls[i:i + batch_size]
            print(f"🔄 Processing server batch {i//batch_size + 1}: {len(batch)} jobs")
            
            future_to_url = {
                executor.submit(scrape_job_worker_server_optimized, job_doc): job_doc
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
                        
                        if completed_count % 200 == 0:  # Server progress
                            print(f"📈 Server progress: {completed_count:,}/{len(unscraped_urls):,}")
                            
                    except Exception as e:
                        stats.increment_error()
                        print(f"❌ Future error: {e}")
            
            except KeyboardInterrupt:
                print("\n🛑 Server interrupted")
                executor.shutdown(wait=False)
                break
    
    # Final server statistics
    stats_data = stats.get_stats()
    final_scraped = job_data_col.count_documents({})
    final_remaining = job_urls_col.count_documents({"scraped": {"$ne": True}})
    
    print(f"\n" + "=" * 60)
    print(f"🎉 SERVER-OPTIMIZED JOB SCRAPING COMPLETE!")
    print(f"📊 Final Server Statistics:")
    print(f"   Jobs processed: {stats_data['processed']:,}")
    print(f"   Successful: {stats_data['success']:,}")
    print(f"   Errors: {stats_data['errors']:,}")
    print(f"   Total time: {stats_data['elapsed']/60:.1f} minutes")
    print(f"   Server rate: {stats_data['rate']:.1f} jobs/sec")
    print(f"   Peak memory: {max(stats.memory_samples) if stats.memory_samples else 0:.1f}GB")
    print(f"\n📈 Database Results:")
    print(f"   Jobs scraped: {final_scraped:,}")
    print(f"   Remaining: {final_remaining:,}")
    print(f"🚀 Next: python main_server_optimized.py --step classify_data")

if __name__ == "__main__":
    def signal_handler(sig, frame):
        print('\n🛑 Server graceful shutdown...')
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    main() 