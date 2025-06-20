import os
import random
import json
import time
import asyncio
import aiohttp
from lxml import html
from pymongo import MongoClient, errors
from dotenv import load_dotenv
import logging
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import platform
import gc

# Set up logging optimized for macOS
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# MacBook Pro optimization - detect hardware and optimize
CPU_CORES = multiprocessing.cpu_count()
IS_APPLE_SILICON = platform.machine() == 'arm64'

# Optimized configuration for MacBook Pro
if IS_APPLE_SILICON:
    MAX_WORKERS = min(CPU_CORES * 4, 32)  # Apple Silicon excels at async I/O
    CONCURRENT_REQUESTS = min(CPU_CORES * 3, 24)
    BATCH_SIZE = 200  # Larger batches for Apple Silicon
    RATE_LIMIT_BASE = 0.6  # Faster processing
else:
    MAX_WORKERS = min(CPU_CORES * 2, 20)  # Intel Macs
    CONCURRENT_REQUESTS = min(CPU_CORES * 2, 16)
    BATCH_SIZE = 150
    RATE_LIMIT_BASE = 1.0

print(f"🚀 MacBook Pro Job Scraper: {CPU_CORES} cores, {MAX_WORKERS} workers, Apple Silicon: {IS_APPLE_SILICON}")

# ZenRows Configuration
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")
if not ZENROWS_API_KEY:
    raise RuntimeError("ZENROWS_API_KEY not found in .env file")

ZENROWS_API_URL = "https://api.zenrows.com/v1/"

class AsyncJobRateLimiter:
    """Ultra-fast rate limiter optimized for MacBook Pro job scraping"""
    def __init__(self, min_delay=0.4, max_delay=1.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.last_requests = {}
        self.lock = asyncio.Lock()
    
    async def wait(self, worker_id):
        async with self.lock:
            now = time.time()
            if worker_id in self.last_requests:
                elapsed = now - self.last_requests[worker_id]
                delay = random.uniform(self.min_delay, self.max_delay)
                if elapsed < delay:
                    await asyncio.sleep(delay - elapsed)
            self.last_requests[worker_id] = time.time()

rate_limiter = AsyncJobRateLimiter(RATE_LIMIT_BASE * 0.7, RATE_LIMIT_BASE * 1.5)

async def async_zenrows_job_request(session, url, worker_id, semaphore):
    """Optimized async job page request for MacBook Pro"""
    async with semaphore:
        await rate_limiter.wait(worker_id)
        
        params = {
            'apikey': ZENROWS_API_KEY,
            'url': url,
            'js_render': 'true',
            'premium_proxy': 'true',
            'wait': '4000',  # Job pages need more time to load
            'wait_for': '[data-test="JobListing"]',
            'block_resources': 'image,stylesheet,font,media'
        }
        
        try:
            async with session.get(ZENROWS_API_URL, params=params, timeout=90) as response:
                if response.status == 200:
                    content = await response.text()
                    return content
                elif response.status == 429:
                    logger.warning(f"[{worker_id}] ⚠️ Rate limit - backing off")
                    await asyncio.sleep(6 + random.uniform(0, 4))
                    return None
                else:
                    logger.error(f"[{worker_id}] ❌ Status {response.status}")
                    return None
                    
        except asyncio.TimeoutError:
            logger.warning(f"[{worker_id}] ⏰ Timeout")
            return None
        except Exception as e:
            logger.error(f"[{worker_id}] ❌ Error: {e}")
            return None

def extract_job_data_optimized(html_content):
    """CPU-optimized job data extraction for MacBook Pro"""
    try:
        tree = html.fromstring(html_content)
        job_data = {}
        
        # Find the main job listing div
        job_div = tree.xpath('//div[@data-test="JobListing"]')
        if not job_div:
            return None
        
        job_div = job_div[0]

        # Extract all data in one pass (more efficient)
        # Company name
        company = job_div.xpath('.//span[contains(@class, "text-sm font-semibold text-black")]/text()')
        job_data["company_name"] = company[0].strip() if company else None

        # Hiring status
        hiring_status = job_div.xpath(".//div[contains(@class, 'flex items-center text-sm font-medium text-pop-green')]/text()")
        job_data["hiring_stat"] = hiring_status

        # Company slogan
        slogan = job_div.xpath(".//div[contains(@class, 'text-sm font-light text-neutral-500')]/text()")
        job_data["slogan"] = slogan

        # Job position/title
        position = job_div.xpath('.//h1[contains(@class, "inline text-xl font-semibold text-black")]/text()')
        job_data["position"] = position[0].strip() if position else None

        # Job details (salary, location, experience) - optimized extraction
        ul_elements = job_div.xpath('.//ul[contains(@class, "block text-md text-black md:flex")]')
        if ul_elements:
            ul = ul_elements[0]
            li_texts = ul.xpath('./li/text()')
            
            if len(li_texts) >= 1:
                job_data["price"] = li_texts[0].strip()
            if len(li_texts) >= 2:
                job_data["location"] = li_texts[1].strip()
            if len(li_texts) >= 3:
                job_data["exp_req"] = li_texts[2].strip()

        # Job description - optimized to avoid deep recursion
        desc_texts = job_div.xpath('.//div[contains(@class, "break-words")]//text()')
        if desc_texts:
            description_parts = [text.strip() for text in desc_texts if text.strip()]
            job_data["job_description"] = ' '.join(description_parts)[:3000]  # Limit size

        # Skills section - batch extraction
        skills_sections = job_div.xpath('.//div[h3[text()="Skills"]]')
        if skills_sections:
            skills = skills_sections[0].xpath('.//span[contains(@class, "text-sm")]/text()')
            job_data["skills"] = [skill.strip() for skill in skills if skill.strip()][:15]  # Limit skills

        # Remote work policy
        remote_texts = job_div.xpath('.//div[contains(@class, "text-sm") and contains(text(), "Remote")]//text()')
        job_data["remote_work_pol"] = ' '.join(remote_texts) if remote_texts else None

        # Visa sponsorship
        visa_texts = job_div.xpath('.//div[contains(text(), "Visa") or contains(text(), "sponsorship")]//text()')
        job_data["visa"] = ' '.join(visa_texts) if visa_texts else None

        # Company details section - batch processing
        company_sections = job_div.xpath('.//div[h3[text()="Company"]]')
        if company_sections:
            company_section = company_sections[0]
            
            # All company info in one pass
            all_company_texts = company_section.xpath('.//text()')
            company_text_joined = ' '.join([t.strip() for t in all_company_texts if t.strip()])
            
            # Extract specific info from combined text
            if 'employees' in company_text_joined.lower() or 'people' in company_text_joined.lower():
                size_match = [t for t in all_company_texts if 'employees' in t.lower() or 'people' in t.lower()]
                job_data["company_size"] = size_match[0][:100] if size_match else None
            
            job_data["company_location"] = company_text_joined[:200]

        # Industries - optimized extraction
        industry_texts = job_div.xpath('.//div[contains(text(), "Industries")]/following-sibling::div//text()')
        job_data["company_industries"] = [ind.strip() for ind in industry_texts if ind.strip()][:5]

        # Funding information
        funding_texts = job_div.xpath('.//div[contains(text(), "Total Raised") or contains(text(), "funding")]//text()')
        if funding_texts:
            job_data["amount_raised"] = ' '.join(funding_texts)[:100]

        # Founder information
        founder_texts = job_div.xpath('.//div[contains(text(), "Founder") or contains(text(), "CEO")]//text()')
        if founder_texts:
            job_data["founder"] = ' '.join(founder_texts)[:100]

        return job_data
        
    except Exception as e:
        logger.error(f"Error extracting job data: {e}")
        return None

async def scrape_single_job_async(session, job_url_doc, worker_id, semaphore, mongo_collection):
    """Async job scraping optimized for MacBook Pro"""
    job_url = job_url_doc["url"]
    
    try:
        # Get job page content with retries
        html_content = None
        for attempt in range(3):
            html_content = await async_zenrows_job_request(session, job_url, worker_id, semaphore)
            if html_content:
                break
            elif attempt < 2:
                await asyncio.sleep((2 ** attempt) + random.uniform(0, 2))
        
        if not html_content:
            return (job_url, False, None, "Failed to get content")
        
        # Extract job data (CPU-intensive, run in thread pool)
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=4) as executor:
            job_data = await loop.run_in_executor(executor, extract_job_data_optimized, html_content)
        
        if not job_data:
            return (job_url, False, None, "Failed to extract data")
        
        # Save to MongoDB with bulk operations
        document = {
            "url": job_url,
            "scraped_at": time.time(),
            "data": job_data
        }
        
        try:
            # Use replace for better performance than update
            mongo_collection.replace_one({"url": job_url}, document, upsert=True)
            return (job_url, True, job_data, None)
            
        except errors.PyMongoError as e:
            return (job_url, False, None, f"MongoDB error: {e}")
            
    except Exception as e:
        return (job_url, False, None, str(e))

async def process_job_batch_async(job_batch, batch_id):
    """Process a batch of jobs with optimal async performance"""
    logger.info(f"📦 Batch {batch_id}: Processing {len(job_batch)} jobs")
    
    # MongoDB connection per batch
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        logger.error("❌ MONGO_URI not found!")
        return []
    
    client = MongoClient(MONGO_URI)
    db = client[os.getenv("MONGO_DB_NAME", "job_scraping")]
    job_data_collection = db["job_data"]
    job_urls_collection = db["job_urls"]
    
    # Create indexes for performance
    job_data_collection.create_index("url", unique=True)
    
    # Semaphore for request limiting
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=120),
        connector=aiohttp.TCPConnector(
            limit=CONCURRENT_REQUESTS + 10,
            limit_per_host=min(CONCURRENT_REQUESTS, 12),
            ttl_dns_cache=300,  # Cache DNS for 5 minutes
            use_dns_cache=True
        )
    ) as session:
        # Create tasks for all jobs in batch
        tasks = []
        for i, job_doc in enumerate(job_batch):
            worker_id = f"{batch_id}-{i}"
            task = scrape_single_job_async(session, job_doc, worker_id, semaphore, job_data_collection)
            tasks.append(task)
        
        # Process all jobs concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Bulk update scraped status for successful jobs
        successful_urls = [
            result[0] for result in results 
            if isinstance(result, tuple) and result[1]  # Success = True
        ]
        
        if successful_urls:
            # Bulk update for better performance
            try:
                job_urls_collection.update_many(
                    {"url": {"$in": successful_urls}},
                    {"$set": {"scraped": True, "processed": True}}
                )
            except Exception as e:
                logger.error(f"Bulk update error: {e}")
        
        successful = sum(1 for r in results if isinstance(r, tuple) and r[1])
        logger.info(f"✅ Batch {batch_id}: {successful}/{len(job_batch)} successful")
    
    client.close()
    
    # Force garbage collection for memory optimization
    gc.collect()
    
    return results

async def main():
    """Main async function optimized for MacBook Pro"""
    print("🚀 MacBook Pro Async Job Data Scraper Starting...")
    print(f"⚙️ Config: {MAX_WORKERS} workers, {CONCURRENT_REQUESTS} concurrent, batch size {BATCH_SIZE}")
    
    # Connect to MongoDB and get unscraped jobs
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB_NAME", "job_scraping")]
    job_urls_collection = db["job_urls"]
    
    # Get unscraped job URLs with projection for memory efficiency
    unscraped_cursor = job_urls_collection.find(
        {"scraped": False}, 
        {"url": 1, "_id": 0}  # Only get URL field
    )
    unscraped_jobs = list(unscraped_cursor)
    total_jobs = len(unscraped_jobs)
    
    if total_jobs == 0:
        print("✅ No unscraped jobs found!")
        client.close()
        return
    
    print(f"📋 Found {total_jobs:,} jobs to scrape")
    
    # Create indexes for better performance
    db["job_data"].create_index("url", unique=True)
    db["job_data"].create_index("scraped_at")
    
    client.close()
    
    # Split into optimized batches
    batch_size = min(BATCH_SIZE, max(50, total_jobs // MAX_WORKERS))
    batches = [unscraped_jobs[i:i+batch_size] for i in range(0, total_jobs, batch_size)]
    
    print(f"📦 Processing {len(batches)} batches of ~{batch_size} jobs each")
    
    start_time = time.time()
    successful_jobs = 0
    
    # Process batches with controlled concurrency
    if IS_APPLE_SILICON:
        max_concurrent_batches = min(len(batches), 8)  # Apple Silicon can handle more
    else:
        max_concurrent_batches = min(len(batches), 6)  # Intel Macs
    
    for i in range(0, len(batches), max_concurrent_batches):
        batch_group = batches[i:i+max_concurrent_batches]
        
        # Process batch group concurrently
        batch_tasks = [
            process_job_batch_async(batch, f"B{i+j}")
            for j, batch in enumerate(batch_group)
        ]
        
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # Count successes
        for batch_result in batch_results:
            if isinstance(batch_result, list):
                batch_successful = sum(1 for r in batch_result if isinstance(r, tuple) and r[1])
                successful_jobs += batch_successful
        
        # Progress update
        processed_batches = min(i + max_concurrent_batches, len(batches))
        processed_jobs = min(processed_batches * batch_size, total_jobs)
        progress = (processed_jobs / total_jobs) * 100
        
        print(f"📊 Progress: {processed_jobs:,}/{total_jobs:,} ({progress:.1f}%) - {successful_jobs:,} successful")
        
        # Brief pause between batch groups (except last)
        if i + max_concurrent_batches < len(batches):
            await asyncio.sleep(3)
    
    # Final statistics
    duration = time.time() - start_time
    
    print(f"\n🎉 MacBook Pro async job scraping completed!")
    print(f"⏱️ Duration: {duration/60:.1f} minutes")
    print(f"✅ Successful jobs: {successful_jobs:,}/{total_jobs:,}")
    print(f"📊 Success rate: {(successful_jobs/total_jobs*100):.1f}%")
    print(f"⚡ Speed: {total_jobs/(duration/60):.1f} jobs/minute")
    
    # Check final MongoDB status
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB_NAME", "job_scraping")]
    
    total_scraped = db["job_data"].count_documents({})
    remaining_unscraped = db["job_urls"].count_documents({"scraped": False})
    
    print(f"\n📈 MongoDB Status:")
    print(f"   Total scraped jobs: {total_scraped:,}")
    print(f"   Remaining unscraped: {remaining_unscraped:,}")
    print(f"🚀 Next: python data_classification_parallel.py")
    
    client.close()

if __name__ == "__main__":
    # MacBook Pro performance optimizations
    if platform.system() == "Darwin":  # macOS
        # Optimize for macOS networking
        import resource
        try:
            # Increase file descriptor limits
            resource.setrlimit(resource.RLIMIT_NOFILE, (10240, 10240))
            
            # If Apple Silicon, optimize for efficiency cores
            if IS_APPLE_SILICON:
                os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
        except:
            pass
    
    # Run the async main function
    asyncio.run(main()) 