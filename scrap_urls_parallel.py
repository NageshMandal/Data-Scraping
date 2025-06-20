import os
import json
import random
import time
import asyncio
import aiohttp
from lxml import html
from pymongo import MongoClient, errors
from dotenv import load_dotenv
import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import platform

# Set up logging with macOS optimization
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# MacBook Pro optimization - detect cores and optimize accordingly
CPU_CORES = multiprocessing.cpu_count()
IS_APPLE_SILICON = platform.machine() == 'arm64'

# Optimized configuration for MacBook Pro
if IS_APPLE_SILICON:
    MAX_WORKERS = min(CPU_CORES * 3, 24)  # Apple Silicon can handle more I/O
    CONCURRENT_REQUESTS = min(CPU_CORES * 2, 16)  # Async requests
    RATE_LIMIT_BASE = 0.8  # Faster on Apple Silicon
else:
    MAX_WORKERS = min(CPU_CORES * 2, 16)  # Intel Macs
    CONCURRENT_REQUESTS = min(CPU_CORES, 12)
    RATE_LIMIT_BASE = 1.2

print(f"🚀 MacBook Pro Optimization: {CPU_CORES} cores, {MAX_WORKERS} workers, Apple Silicon: {IS_APPLE_SILICON}")

# ZenRows Configuration
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")
if not ZENROWS_API_KEY:
    raise RuntimeError("ZENROWS_API_KEY not found in .env file")

ZENROWS_API_URL = "https://api.zenrows.com/v1/"

class AsyncRateLimiter:
    """High-performance async rate limiter for MacBook Pro"""
    def __init__(self, min_delay=0.5, max_delay=1.5):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.last_requests = {}
        self.lock = asyncio.Lock()
    
    async def wait(self, worker_id=None):
        async with self.lock:
            now = time.time()
            if worker_id in self.last_requests:
                elapsed = now - self.last_requests[worker_id]
                delay = random.uniform(self.min_delay, self.max_delay)
                if elapsed < delay:
                    sleep_time = delay - elapsed
                    await asyncio.sleep(sleep_time)
            self.last_requests[worker_id] = time.time()

rate_limiter = AsyncRateLimiter(RATE_LIMIT_BASE, RATE_LIMIT_BASE * 2)

async def async_zenrows_request(session, url, worker_id, semaphore):
    """Async ZenRows request optimized for MacBook Pro"""
    async with semaphore:
        await rate_limiter.wait(worker_id)
        
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
            async with session.get(ZENROWS_API_URL, params=params, timeout=60) as response:
                if response.status == 200:
                    content = await response.text()
                    logger.info(f"[Worker-{worker_id}] ✅ Success: {len(content)} chars")
                    return content
                elif response.status == 429:
                    logger.warning(f"[Worker-{worker_id}] ⚠️ Rate limit, backing off")
                    await asyncio.sleep(5 + random.uniform(0, 3))
                    return None
                else:
                    logger.error(f"[Worker-{worker_id}] ❌ Status {response.status}")
                    return None
                    
        except asyncio.TimeoutError:
            logger.warning(f"[Worker-{worker_id}] ⏰ Timeout")
            return None
        except Exception as e:
            logger.error(f"[Worker-{worker_id}] ❌ Error: {e}")
            return None

def extract_urls_from_html(html_content, base_url):
    """CPU-intensive HTML parsing - can be parallelized"""
    try:
        tree = html.fromstring(html_content)
        hrefs = tree.xpath('//a[contains(@class, "mr-2") and contains(@class, "text-brand-burgandy")]/@href')
        urls = [
            (base_url.split("/role")[0] + href) if href.startswith("/") else href
            for href in hrefs
        ]
        return urls
    except Exception as e:
        logger.error(f"HTML parsing error: {e}")
        return []

async def scrape_url_pages_async(session, base_url, worker_id, semaphore, mongo_collection):
    """Async scraping of all pages for a URL"""
    job_count = 0
    page = 1
    
    while True:
        page_url = base_url if page == 1 else f"{base_url}?page={page}"
        logger.info(f"[Worker-{worker_id}] 🕷️ Page {page}: {page_url}")
        
        # Try multiple times with exponential backoff
        html_content = None
        for attempt in range(3):
            html_content = await async_zenrows_request(session, page_url, worker_id, semaphore)
            if html_content:
                break
            elif attempt < 2:
                wait_time = (2 ** attempt) + random.uniform(0, 2)
                await asyncio.sleep(wait_time)
        
        if not html_content:
            logger.warning(f"[Worker-{worker_id}] Failed to get page {page}")
            break
            
        # Check for termination conditions
        if "Page not found (404)" in html_content or "0 results total" in html_content:
            logger.info(f"[Worker-{worker_id}] End of results at page {page}")
            break
        
        # Extract URLs (CPU-intensive, run in thread pool)
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=4) as executor:
            urls = await loop.run_in_executor(executor, extract_urls_from_html, html_content, base_url)
        
        if not urls:
            logger.info(f"[Worker-{worker_id}] No URLs found on page {page}")
            break
        
        # Bulk insert to MongoDB (more efficient)
        if urls:
            documents = [
                {"url": url, "processed": False, "scraped": False}
                for url in urls
            ]
            
            try:
                # Use ordered=False for better performance
                mongo_collection.insert_many(documents, ordered=False)
                job_count += len(urls)
                logger.info(f"[Worker-{worker_id}] ✅ Page {page}: {len(urls)} URLs, {job_count} total")
            except errors.BulkWriteError as e:
                # Handle duplicates gracefully
                inserted = e.details.get('nInserted', 0)
                job_count += inserted
                logger.info(f"[Worker-{worker_id}] ✅ Page {page}: {inserted} new URLs, {job_count} total")
        
        page += 1
        await asyncio.sleep(random.uniform(0.5, 1.5))  # Brief pause between pages
    
    return base_url, job_count

async def process_urls_batch(urls_batch, batch_id):
    """Process a batch of URLs asynchronously"""
    logger.info(f"📦 Starting batch {batch_id} with {len(urls_batch)} URLs")
    
    # MongoDB connection (one per batch)
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        logger.error("❌ MONGO_URI not found!")
        return []
    
    client = MongoClient(MONGO_URI)
    db = client[os.getenv("MONGO_DB_NAME", "job_scraping")]
    collection = db["job_urls"]
    collection.create_index("url", unique=True)
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=90),
        connector=aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, limit_per_host=8)
    ) as session:
        # Create tasks for all URLs in batch
        tasks = []
        for i, url_entry in enumerate(urls_batch):
            if not url_entry.get("value", False):  # Only unprocessed URLs
                worker_id = f"{batch_id}-{i}"
                task = scrape_url_pages_async(session, url_entry["url"], worker_id, semaphore, collection)
                tasks.append(task)
        
        # Process all URLs concurrently
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Update progress in wellfound_urls.json
            for i, url_entry in enumerate(urls_batch):
                if not url_entry.get("value", False):
                    url_entry["value"] = True
            
            successful = sum(1 for r in results if not isinstance(r, Exception))
            total_jobs = sum(r[1] for r in results if isinstance(r, tuple) and len(r) == 2)
            
            logger.info(f"✅ Batch {batch_id}: {successful}/{len(tasks)} successful, {total_jobs} jobs found")
    
    client.close()
    return results

def save_progress(urls_data):
    """Save progress to file efficiently"""
    try:
        with open("wellfound_urls.json", "w") as f:
            json.dump(urls_data, f, indent=2)
        logger.info("💾 Progress saved")
    except Exception as e:
        logger.error(f"❌ Error saving progress: {e}")

async def main():
    """Main async function optimized for MacBook Pro"""
    print("🚀 MacBook Pro Async URL Scraper Starting...")
    print(f"⚙️ Config: {MAX_WORKERS} workers, {CONCURRENT_REQUESTS} concurrent requests")
    
    # Load URLs
    if not os.path.exists("wellfound_urls.json"):
        print("❌ wellfound_urls.json not found. Run: python build_url.py")
        return
    
    with open("wellfound_urls.json", "r") as f:
        urls_data = json.load(f)
    
    # Filter unprocessed URLs
    unprocessed = [url for url in urls_data if not url.get("value", False)]
    if not unprocessed:
        print("✅ All URLs already processed!")
        return
    
    print(f"📋 Found {len(unprocessed):,} URLs to process")
    
    # Split into batches for optimal processing
    batch_size = max(50, len(unprocessed) // MAX_WORKERS)  # Dynamic batch sizing
    batches = [unprocessed[i:i+batch_size] for i in range(0, len(unprocessed), batch_size)]
    
    print(f"📦 Processing {len(batches)} batches of ~{batch_size} URLs each")
    
    start_time = time.time()
    
    # Process batches with controlled parallelism
    if IS_APPLE_SILICON:
        # Apple Silicon can handle more concurrent batches
        max_concurrent_batches = min(len(batches), 6)
    else:
        max_concurrent_batches = min(len(batches), 4)
    
    for i in range(0, len(batches), max_concurrent_batches):
        batch_group = batches[i:i+max_concurrent_batches]
        
        # Process batch group concurrently
        batch_tasks = [
            process_urls_batch(batch, f"B{i+j}")
            for j, batch in enumerate(batch_group)
        ]
        
        await asyncio.gather(*batch_tasks)
        
        # Save progress after each batch group
        save_progress(urls_data)
        
        # Brief pause between batch groups
        if i + max_concurrent_batches < len(batches):
            await asyncio.sleep(2)
    
    # Final statistics
    duration = time.time() - start_time
    
    # Check MongoDB final count
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB_NAME", "job_scraping")]
    total_urls = db["job_urls"].count_documents({})
    unscraped = db["job_urls"].count_documents({"scraped": False})
    client.close()
    
    print(f"\n🎉 MacBook Pro async scraping completed!")
    print(f"⏱️ Duration: {duration/60:.1f} minutes")
    print(f"📊 Total URLs in database: {total_urls:,}")
    print(f"📋 Ready for job scraping: {unscraped:,}")
    print(f"⚡ Speed: {len(unprocessed)/(duration/60):.1f} URLs/minute")
    print(f"🚀 Next: python scrap_jobData_parallel.py")

if __name__ == "__main__":
    # MacBook Pro performance optimization
    if platform.system() == "Darwin":  # macOS
        # Increase file descriptor limits for better networking
        import resource
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (8192, 8192))
        except:
            pass
    
    # Run the async main function
    asyncio.run(main()) 