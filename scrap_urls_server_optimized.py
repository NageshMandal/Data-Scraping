#!/usr/bin/env python3
"""
Server-Optimized URL Scraper with Maximum Concurrency
====================================================

Ultra high-performance URL scraping optimized for professional server:
- 92GB RAM, 16 cores/32 threads
- 40 concurrent workers
- 25 req/sec rate limiting
- 2GB memory buffer
- Advanced resource management
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
import psutil

# Load environment variables
load_dotenv()

# Load server configuration
def load_server_config():
    config_path = os.getenv('SERVER_CONFIG_PATH', 'config/server_config.json')
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            return config["performance_config"]["url_scraping"]
    except:
        # Fallback to server-optimized defaults
        return {
            "max_workers": 40,
            "rate_limit_per_second": 25,
            "batch_size": 200,
            "max_retries": 3,
            "timeout": 90,
            "memory_buffer_mb": 2048
        }

# ——— Server Performance Configuration ———
server_config = load_server_config()
MAX_WORKERS = server_config["max_workers"]
RATE_LIMIT_PER_SECOND = server_config["rate_limit_per_second"]
BATCH_SIZE = server_config["batch_size"]
MAX_RETRIES = server_config["max_retries"]
TIMEOUT = server_config["timeout"]
MEMORY_BUFFER_MB = server_config["memory_buffer_mb"]

print(f"🔥 SERVER-OPTIMIZED URL SCRAPER")
print(f"   Workers: {MAX_WORKERS} (Server-grade concurrency)")
print(f"   Rate Limit: {RATE_LIMIT_PER_SECOND} req/sec")
print(f"   Batch Size: {BATCH_SIZE}")
print(f"   Memory Buffer: {MEMORY_BUFFER_MB}MB")

# ——— Advanced Rate Limiter for Server Load ———
class ServerRateLimiter:
    def __init__(self, max_calls_per_second):
        self.max_calls = max_calls_per_second
        self.calls = []
        self.lock = Lock()
        self.adaptive_factor = 1.0  # For adaptive rate limiting
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            # Remove calls older than 1 second
            self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            # Adaptive rate limiting based on server load
            cpu_usage = psutil.cpu_percent(interval=0.1)
            if cpu_usage > 80:
                self.adaptive_factor = 0.7  # Slow down if CPU high
            elif cpu_usage < 50:
                self.adaptive_factor = 1.2  # Speed up if CPU low
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

# Global rate limiter instance
rate_limiter = ServerRateLimiter(RATE_LIMIT_PER_SECOND)

# ——— ZenRows Configuration ———
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")
if not ZENROWS_API_KEY:
    raise RuntimeError("ZENROWS_API_KEY not found in .env file")

ZENROWS_API_URL = "https://api.zenrows.com/v1/"

# ——— Advanced Statistics Tracking ———
class ServerStatsTracker:
    def __init__(self):
        self.lock = Lock()
        self.total_processed = 0
        self.total_success = 0
        self.total_errors = 0
        self.total_jobs_found = 0
        self.start_time = time.time()
        self.memory_usage_samples = []
        self.cpu_usage_samples = []
        
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
    
    def sample_resources(self):
        """Sample server resource usage"""
        with self.lock:
            memory_mb = psutil.virtual_memory().used / (1024 * 1024)
            cpu_percent = psutil.cpu_percent()
            
            self.memory_usage_samples.append(memory_mb)
            self.cpu_usage_samples.append(cpu_percent)
            
            # Keep only last 100 samples
            if len(self.memory_usage_samples) > 100:
                self.memory_usage_samples.pop(0)
            if len(self.cpu_usage_samples) > 100:
                self.cpu_usage_samples.pop(0)
            
    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            avg_memory = sum(self.memory_usage_samples) / len(self.memory_usage_samples) if self.memory_usage_samples else 0
            avg_cpu = sum(self.cpu_usage_samples) / len(self.cpu_usage_samples) if self.cpu_usage_samples else 0
            
            return {
                'processed': self.total_processed,
                'success': self.total_success,
                'errors': self.total_errors,
                'jobs_found': self.total_jobs_found,
                'elapsed': elapsed,
                'rate': self.total_processed / elapsed if elapsed > 0 else 0,
                'avg_memory_mb': avg_memory,
                'avg_cpu_percent': avg_cpu,
                'current_memory_gb': psutil.virtual_memory().used / (1024**3),
                'available_memory_gb': psutil.virtual_memory().available / (1024**3)
            }

stats = ServerStatsTracker()

def server_optimized_zenrows_request(url, timeout=TIMEOUT):
    """
    Server-optimized ZenRows request with advanced error handling
    """
    rate_limiter.wait_if_needed()
    
    params = {
        'apikey': ZENROWS_API_KEY,
        'url': url,
        'js_render': 'true',
        'premium_proxy': 'true',
        'wait': '2000',
        'wait_for': '.text-brand-burgandy',
        'block_resources': 'image,stylesheet,font,media'
    }
    
    try:
        response = requests.get(ZENROWS_API_URL, params=params, timeout=timeout)
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 429:
            # Rate limit hit - adaptive backoff
            backoff_time = random.uniform(8, 15)
            time.sleep(backoff_time)
            return None
        else:
            return None
            
    except requests.exceptions.Timeout:
        return None
    except Exception:
        return None

# ——— MongoDB setup with server-optimized connection pooling ———
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("❌ ERROR: MONGO_URI not found in environment variables!")
    exit(1)
DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")

# Thread-local MongoDB connections with server optimization
thread_local = threading.local()

def get_mongo_connection():
    """Get thread-local MongoDB connection with server optimization"""
    if not hasattr(thread_local, 'client'):
        thread_local.client = MongoClient(
            MONGO_URI,
            maxPoolSize=100,  # Server-grade connection pool
            minPoolSize=50,
            socketTimeoutMS=120000,
            connectTimeoutMS=30000,
            serverSelectionTimeoutMS=30000
        )
        thread_local.db = thread_local.client[DB_NAME]
        thread_local.collection = thread_local.db["job_urls"]
        thread_local.collection.create_index("url", unique=True)
    return thread_local.collection

# ——— Server-Optimized Batch URL processor ———
class ServerBatchURLProcessor:
    def __init__(self, batch_size=BATCH_SIZE):
        self.batch_size = batch_size
        self.batch = []
        self.lock = Lock()
        self.total_batches_processed = 0
        
    def add_url(self, url):
        with self.lock:
            self.batch.append({
                "url": url,
                "processed": False,
                "scraped": False,
                "discovered_at": time.time()
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
                self.total_batches_processed += 1
                
                # Log progress for large batches
                if self.total_batches_processed % 10 == 0:
                    print(f"📦 Processed {self.total_batches_processed} batches ({self.total_batches_processed * self.batch_size:,} URLs)")
                    
        except Exception as e:
            print(f"⚠️ Batch write error: {e}")
            
        self.batch.clear()
        
    def flush(self):
        with self.lock:
            self._flush_batch()

batch_processor = ServerBatchURLProcessor()

def scrape_single_page_server_optimized(url, page=1):
    """
    Server-optimized single page scraper with enhanced error handling
    """
    page_url = url if page == 1 else f"{url}?page={page}"
    
    for attempt in range(MAX_RETRIES):
        html_src = server_optimized_zenrows_request(page_url)
        
        if html_src is None:
            if attempt < MAX_RETRIES - 1:
                backoff_time = min(2 ** attempt + random.uniform(0, 1), 10)
                time.sleep(backoff_time)
            continue
            
        # Enhanced failure detection
        failure_indicators = [
            "Page not found (404)",
            "0 results total",
            "No jobs found",
            "Access denied",
            "<title>Error</title>"
        ]
        
        if any(indicator in html_src for indicator in failure_indicators):
            return False, 0, "Page error or no results"
            
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
                time.sleep(random.uniform(1, 3))
                continue
            return False, 0, f"Parse error: {e}"
    
    return False, 0, "Max retries exceeded"

def scrape_url_worker_server_optimized(url_entry):
    """
    Server-optimized worker function with enhanced pagination and resource monitoring
    """
    base_url = url_entry["url"]
    thread_id = threading.current_thread().ident
    worker_start_time = time.time()
    
    try:
        total_jobs = 0
        page = 1
        consecutive_failures = 0
        max_pages = 100  # Server can handle more pages
        
        while consecutive_failures < 3 and page <= max_pages:
            # Sample resources periodically
            if page % 10 == 0:
                stats.sample_resources()
            
            success, jobs_count, error = scrape_single_page_server_optimized(base_url, page)
            
            if success:
                total_jobs += jobs_count
                consecutive_failures = 0
                
                if page % 5 == 0:  # Log every 5th page for server monitoring
                    print(f"🔧 Thread-{thread_id % 1000:03d} | Page {page} | {base_url.split('/')[-2:]}: {jobs_count} jobs (Total: {total_jobs})")
                
                # Advanced pagination logic for server performance
                if jobs_count < 3:  # Very few jobs, likely near end
                    break
                elif jobs_count < 10 and page > 20:  # Diminishing returns
                    break
                    
                page += 1
                
                # Adaptive delay based on server load
                cpu_usage = psutil.cpu_percent(interval=0.1)
                if cpu_usage > 80:
                    time.sleep(random.uniform(2, 4))  # Slower when CPU high
                else:
                    time.sleep(random.uniform(0.5, 1.5))  # Faster when CPU low
                
            else:
                consecutive_failures += 1
                if error in ["Page error or no results", "404 Not Found"]:
                    break  # Terminal errors
                    
                if consecutive_failures < 3:
                    backoff_time = min(consecutive_failures * 2, 8)
                    time.sleep(random.uniform(backoff_time, backoff_time * 1.5))
        
        worker_duration = time.time() - worker_start_time
        stats.increment_success(total_jobs)
        
        # Log worker completion for server monitoring
        if total_jobs > 50 or worker_duration > 300:  # Log significant workers
            print(f"🏁 Worker-{thread_id % 1000:03d} completed: {total_jobs} jobs in {worker_duration/60:.1f}min")
        
        return True, total_jobs
        
    except Exception as e:
        stats.increment_error()
        print(f"❌ Worker error for {base_url}: {e}")
        return False, 0

def server_progress_reporter():
    """Advanced progress reporter with server metrics"""
    while True:
        time.sleep(30)  # Report every 30 seconds
        stats.sample_resources()
        stats_data = stats.get_stats()
        
        print(f"\n📊 SERVER PERFORMANCE REPORT:")
        print(f"   Processed: {stats_data['processed']}/11600")  
        print(f"   Success: {stats_data['success']} | Errors: {stats_data['errors']}")
        print(f"   Jobs found: {stats_data['jobs_found']:,}")
        print(f"   Processing rate: {stats_data['rate']:.1f} URLs/sec")
        print(f"   Server RAM: {stats_data['current_memory_gb']:.1f}GB used / {stats_data['available_memory_gb']:.1f}GB available")
        print(f"   Server CPU: {stats_data['avg_cpu_percent']:.1f}% average")
        print(f"   Runtime: {stats_data['elapsed']/60:.1f} minutes\n")

# ——— Main server-optimized processing ———
def main():
    URLS_FILE = os.path.join(os.path.dirname(__file__), "wellfound_urls.json")
    
    if not os.path.exists(URLS_FILE):
        print(f"❌ URLs file not found: {URLS_FILE}")
        print("Please run: python main_server_optimized.py --step generate_urls first")
        return
    
    # Load URLs
    with open(URLS_FILE, "r") as f:
        targets = json.load(f)
    
    # Filter unprocessed URLs
    unprocessed = [entry for entry in targets if not entry.get("value", False)]
    
    print(f"🚀 SERVER-OPTIMIZED URL SCRAPER STARTING")
    print(f"🔥 MAXIMUM PERFORMANCE MODE")
    print(f"📊 Total URLs: {len(targets):,}")
    print(f"📋 Unprocessed: {len(unprocessed):,}")
    print(f"🔧 Max Workers: {MAX_WORKERS} (Server-grade)")  
    print(f"⚡ Rate Limit: {RATE_LIMIT_PER_SECOND} req/sec (Adaptive)")
    print(f"💾 Memory Buffer: {MEMORY_BUFFER_MB}MB")
    print(f"🖥️ Server RAM: {psutil.virtual_memory().total / (1024**3):.1f}GB total")
    print("=" * 70)
    
    if not unprocessed:
        print("✅ All URLs already processed!")
        return
    
    # Start server progress reporter
    progress_thread = threading.Thread(target=server_progress_reporter, daemon=True)
    progress_thread.start()
    
    # Process URLs with maximum server concurrency
    completed_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all unprocessed URLs
        future_to_url = {
            executor.submit(scrape_url_worker_server_optimized, url_entry): url_entry 
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
                        
                        # Save progress more frequently for server mode
                        if completed_count % 50 == 0:  # Every 50 completions
                            with open(URLS_FILE, "w") as f:
                                json.dump(targets, f, indent=2)
                            print(f"💾 Server progress saved: {completed_count}/{len(unprocessed)}")
                            
                except Exception as e:
                    print(f"❌ Future error: {e}")
                    stats.increment_error()
        
        except KeyboardInterrupt:
            print("\n🛑 Server interrupted by user")
            executor.shutdown(wait=False)
    
    # Final cleanup
    batch_processor.flush()
    
    # Save final results
    with open(URLS_FILE, "w") as f:
        json.dump(targets, f, indent=2)
    
    # Final server statistics
    stats_data = stats.get_stats()
    collection = get_mongo_connection()
    total_urls = collection.count_documents({})
    unscraped = collection.count_documents({"scraped": False})
    
    print(f"\n" + "=" * 70)
    print(f"🎉 SERVER-OPTIMIZED SCRAPING COMPLETE!")
    print(f"📊 Final Server Statistics:")
    print(f"   URLs processed: {stats_data['processed']:,}")
    print(f"   Successful: {stats_data['success']:,}")
    print(f"   Errors: {stats_data['errors']:,}")
    print(f"   Job URLs discovered: {stats_data['jobs_found']:,}")
    print(f"   Total time: {stats_data['elapsed']/60:.1f} minutes")
    print(f"   Average rate: {stats_data['rate']:.1f} URLs/sec")
    print(f"   Peak memory usage: {max(stats_data.get('memory_usage_samples', [0]))/1024:.1f}GB")
    print(f"   Average CPU usage: {stats_data['avg_cpu_percent']:.1f}%")
    print(f"\n📈 Database Results:")
    print(f"   Total URLs in MongoDB: {total_urls:,}")
    print(f"   Ready for job scraping: {unscraped:,}")
    print(f"🚀 Next step: python main_server_optimized.py --step scrape_jobs")

if __name__ == "__main__":
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print('\n🛑 Server graceful shutdown...')
        batch_processor.flush()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    main() 