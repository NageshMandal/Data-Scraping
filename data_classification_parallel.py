import os
import json
import time
import re
import asyncio
from pymongo import MongoClient, errors
from dotenv import load_dotenv
import logging
from groq import Groq
import random
import multiprocessing
import platform
import gc
from concurrent.futures import ThreadPoolExecutor

# Set up logging optimized for macOS
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# MacBook Pro optimization - AI workloads
CPU_CORES = multiprocessing.cpu_count()
IS_APPLE_SILICON = platform.machine() == 'arm64'

# Optimized configuration for MacBook Pro AI classification
if IS_APPLE_SILICON:
    MAX_WORKERS = min(CPU_CORES * 2, 16)  # Apple Silicon neural engine helps
    CONCURRENT_REQUESTS = min(CPU_CORES, 12)
    BATCH_SIZE = 100  # Larger batches for Apple Silicon
    RATE_LIMIT_BASE = 0.8  # Faster AI processing
else:
    MAX_WORKERS = min(CPU_CORES, 10)  # Intel Macs
    CONCURRENT_REQUESTS = min(CPU_CORES, 8)
    BATCH_SIZE = 75
    RATE_LIMIT_BASE = 1.2

print(f"🚀 MacBook Pro AI Classifier: {CPU_CORES} cores, {MAX_WORKERS} workers, Apple Silicon: {IS_APPLE_SILICON}")

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("❌ ERROR: MONGO_URI not found in environment variables!")
    exit(1)
DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")

# Groq API setup
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    print("❌ ERROR: GROQ_API_KEY not found in environment variables!")
    exit(1)

class AsyncGroqRateLimiter:
    """Ultra-fast rate limiter optimized for MacBook Pro Groq API calls"""
    def __init__(self, min_delay=0.5, max_delay=1.2):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.last_requests = {}
        self.lock = asyncio.Lock()
        self.request_count = 0
        self.start_time = time.time()
    
    async def wait(self, worker_id):
        async with self.lock:
            now = time.time()
            
            # Adaptive rate limiting based on success rate
            self.request_count += 1
            elapsed_total = now - self.start_time
            current_rate = self.request_count / elapsed_total if elapsed_total > 0 else 0
            
            # Adjust delay based on current rate (targeting ~8-12 requests/second max)
            if current_rate > 10:
                multiplier = 1.5
            elif current_rate > 8:
                multiplier = 1.2
            else:
                multiplier = 1.0
            
            if worker_id in self.last_requests:
                elapsed = now - self.last_requests[worker_id]
                delay = random.uniform(self.min_delay, self.max_delay) * multiplier
                if elapsed < delay:
                    await asyncio.sleep(delay - elapsed)
            
            self.last_requests[worker_id] = time.time()

groq_rate_limiter = AsyncGroqRateLimiter(RATE_LIMIT_BASE * 0.6, RATE_LIMIT_BASE * 1.4)

async def classify_job_async(job_data, worker_id, semaphore):
    """Async job classification optimized for MacBook Pro"""
    async with semaphore:
        await groq_rate_limiter.wait(worker_id)
        
        try:
            # Create Groq client (lightweight, can be created per request)
            client = Groq(api_key=GROQ_API_KEY)
            
            # Build optimized classification prompt
            company_name = job_data.get("company_name", "Unknown Company")
            position = job_data.get("position", "Unknown Position")
            description = job_data.get("job_description", "")[:1500]  # Optimized length
            
            industries = job_data.get("company_industries", [])
            industries_str = ", ".join(industries[:3]) if industries else "Not specified"
            
            skills = job_data.get("skills", [])
            skills_str = ", ".join(skills[:8]) if skills else "Not specified"
            
            funding = job_data.get("amount_raised", "Not specified")
            company_size = job_data.get("company_size", "Not specified")
            
            # Optimized prompt for faster processing
            prompt = f"""Analyze this job posting and return valid JSON classification:

Company: {company_name}
Position: {position}
Industries: {industries_str}
Skills: {skills_str}
Size: {company_size}
Funding: {funding}
Description: {description}

Return ONLY this JSON structure:
{{
    "primary_categories": ["AI/ML", "SaaS"],
    "company_stage": "Series B",
    "hiring_urgency": "High",
    "focus_areas": {{
        "technical": ["Python", "React"],
        "business": ["B2B", "Enterprise"]
    }},
    "role_analysis": {{
        "department": "Engineering",
        "seniority_level": "Senior",
        "remote_friendly": true
    }}
}}

Categories: AI/ML, SaaS, E-commerce, FinTech, HealthTech, EdTech, Enterprise Software, Consumer Tech, Developer Tools, Data Analytics, Cybersecurity, Hardware, Mobile, Web Development, Cloud/Infrastructure

Stages: Seed, Series A, Series B, Series C+, Growth Stage, Scale Stage, Public, Bootstrapped

Urgency: Low, Medium, High, Critical

Return only valid JSON, no text."""

            # Use async-compatible call (run in thread pool)
            loop = asyncio.get_event_loop()
            
            def make_groq_request():
                return client.chat.completions.create(
                    messages=[{"role": "user", "content": prompt}],
                    model="llama-3.3-70b-versatile",
                    temperature=0.1,
                    max_tokens=800  # Reduced for faster processing
                )
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                response = await loop.run_in_executor(executor, make_groq_request)
            
            result_text = response.choices[0].message.content.strip()
            
            # Optimized JSON parsing
            try:
                classification = json.loads(result_text)
                return classification
            except json.JSONDecodeError:
                # Fast JSON extraction with regex
                json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', result_text, re.DOTALL)
                if json_match:
                    try:
                        classification = json.loads(json_match.group())
                        return classification
                    except json.JSONDecodeError:
                        pass
                
                logger.warning(f"[{worker_id}] Failed to parse JSON from Groq")
                return None
                
        except Exception as e:
            logger.error(f"[{worker_id}] Groq API error: {e}")
            return None

def generate_prospecting_intel_optimized(job_data, classification):
    """Optimized prospecting intelligence generation"""
    try:
        intel = {}
        
        # Fast funding analysis
        funding = job_data.get("amount_raised", "").lower()
        company_stage = classification.get("company_stage", "")
        
        if any(term in funding for term in ["series", "million", "raised", "$"]):
            if company_stage in ["Series A", "Series B"]:
                intel["investment_readiness"] = "High - Early Growth"
            elif company_stage in ["Series C+", "Growth Stage"]:
                intel["investment_readiness"] = "Medium - Scaling"
            else:
                intel["investment_readiness"] = "High - Funded"
        else:
            intel["investment_readiness"] = "Unknown"
        
        # Fast technology extraction
        skills = job_data.get("skills", [])
        key_techs = set()
        tech_keywords = {"python", "javascript", "react", "aws", "ai", "ml", "data", "node", "vue", "angular"}
        
        for skill in skills[:8]:  # Limit for performance
            skill_lower = skill.lower()
            if any(tech in skill_lower for tech in tech_keywords):
                key_techs.add(skill)
        
        intel["key_technologies"] = list(key_techs)[:5]
        
        # Fast contact potential assessment
        hiring_urgency = classification.get("hiring_urgency", "")
        company_size = job_data.get("company_size", "").lower()
        
        if hiring_urgency in ["High", "Critical"]:
            intel["contact_potential"] = "High - Urgent Hiring"
        elif "startup" in company_size or company_stage in ["Seed", "Series A"]:
            intel["contact_potential"] = "High - Growing Team"
        else:
            intel["contact_potential"] = "Medium"
        
        # Fast hiring volume estimation
        if hiring_urgency in ["High", "Critical"]:
            intel["hiring_volume"] = "High"
        elif company_stage in ["Growth Stage", "Scale Stage"]:
            intel["hiring_volume"] = "Medium-High"
        else:
            intel["hiring_volume"] = "Medium"
        
        # Fast domain estimation
        company_name = job_data.get("company_name", "")
        if company_name:
            clean_name = re.sub(r'[^a-zA-Z0-9]', '', company_name.lower())
            intel["company_domain"] = f"{clean_name}.com"
        
        return intel
        
    except Exception as e:
        logger.error(f"Error generating prospecting intel: {e}")
        return {}

async def classify_single_job_async(job_doc, worker_id, semaphore, mongo_collection):
    """Async single job classification for MacBook Pro"""
    job_url = job_doc["url"]
    job_data = job_doc.get("data", {})
    
    try:
        # Classify with retries
        classification = None
        for attempt in range(3):
            classification = await classify_job_async(job_data, worker_id, semaphore)
            if classification:
                break
            elif attempt < 2:
                wait_time = (2 ** attempt) + random.uniform(0.5, 1.5)
                await asyncio.sleep(wait_time)
        
        if not classification:
            return (job_url, False, None, "Failed to get classification")
        
        # Generate prospecting intelligence (CPU-intensive, run in thread pool)
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=2) as executor:
            prospecting_intel = await loop.run_in_executor(
                executor, generate_prospecting_intel_optimized, job_data, classification
            )
        
        # Create optimized document
        classified_doc = {
            "url": job_url,
            "original_data": job_data,
            "classification": classification,
            "prospecting_intel": prospecting_intel,
            "classified_at": time.time(),
            "model_used": "llama-3.3-70b-versatile"
        }
        
        try:
            # Use replace for better performance
            mongo_collection.replace_one({"url": job_url}, classified_doc, upsert=True)
            
            company = job_data.get("company_name", "Unknown")
            categories = classification.get("primary_categories", [])
            return (job_url, True, {"company": company, "categories": categories}, None)
            
        except errors.PyMongoError as e:
            return (job_url, False, None, f"MongoDB error: {e}")
            
    except Exception as e:
        return (job_url, False, None, str(e))

async def process_classification_batch_async(job_batch, batch_id):
    """Process a batch of classifications with optimal async performance"""
    logger.info(f"📦 Batch {batch_id}: Classifying {len(job_batch)} jobs")
    
    # MongoDB connection per batch
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    classified_collection = db["classified_jobs"]
    
    # Create indexes for performance
    classified_collection.create_index("url", unique=True)
    
    # Semaphore for API limiting
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    # Create tasks for all jobs in batch
    tasks = []
    for i, job_doc in enumerate(job_batch):
        worker_id = f"{batch_id}-{i}"
        task = classify_single_job_async(job_doc, worker_id, semaphore, classified_collection)
        tasks.append(task)
    
    # Process all classifications concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    successful = sum(1 for r in results if isinstance(r, tuple) and r[1])
    logger.info(f"✅ Batch {batch_id}: {successful}/{len(job_batch)} successful")
    
    client.close()
    
    # Memory optimization
    gc.collect()
    
    return results

async def main():
    """Main async function optimized for MacBook Pro AI classification"""
    print("🚀 MacBook Pro Async AI Classification Starting...")
    print(f"⚙️ Config: {MAX_WORKERS} workers, {CONCURRENT_REQUESTS} concurrent, batch size {BATCH_SIZE}")
    
    # Connect to MongoDB and get unclassified jobs
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    job_data_collection = db["job_data"]
    classified_collection = db["classified_jobs"]
    
    # Get all job URLs that have been scraped but not classified
    scraped_jobs = list(job_data_collection.find({}, {"url": 1, "data": 1, "_id": 0}))
    
    # Filter out already classified jobs
    classified_urls = set(doc["url"] for doc in classified_collection.find({}, {"url": 1, "_id": 0}))
    unclassified_jobs = [job for job in scraped_jobs if job["url"] not in classified_urls]
    
    total_classifications = len(unclassified_jobs)
    
    if total_classifications == 0:
        print("✅ No unclassified jobs found!")
        client.close()
        return
    
    print(f"📋 Found {total_classifications:,} jobs to classify")
    
    # Create indexes for better performance
    classified_collection.create_index("classified_at")
    
    client.close()
    
    # Split into optimized batches
    batch_size = min(BATCH_SIZE, max(25, total_classifications // MAX_WORKERS))
    batches = [unclassified_jobs[i:i+batch_size] for i in range(0, total_classifications, batch_size)]
    
    print(f"📦 Processing {len(batches)} batches of ~{batch_size} jobs each")
    
    start_time = time.time()
    successful_classifications = 0
    
    # Process batches with controlled concurrency
    if IS_APPLE_SILICON:
        max_concurrent_batches = min(len(batches), 6)  # Apple Silicon neural engine
    else:
        max_concurrent_batches = min(len(batches), 4)  # Intel Macs
    
    for i in range(0, len(batches), max_concurrent_batches):
        batch_group = batches[i:i+max_concurrent_batches]
        
        # Process batch group concurrently
        batch_tasks = [
            process_classification_batch_async(batch, f"B{i+j}")
            for j, batch in enumerate(batch_group)
        ]
        
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        # Count successes
        for batch_result in batch_results:
            if isinstance(batch_result, list):
                batch_successful = sum(1 for r in batch_result if isinstance(r, tuple) and r[1])
                successful_classifications += batch_successful
        
        # Progress update
        processed_batches = min(i + max_concurrent_batches, len(batches))
        processed_jobs = min(processed_batches * batch_size, total_classifications)
        progress = (processed_jobs / total_classifications) * 100
        
        print(f"📊 Progress: {processed_jobs:,}/{total_classifications:,} ({progress:.1f}%) - {successful_classifications:,} successful")
        
        # Longer pause between batch groups for API respect
        if i + max_concurrent_batches < len(batches):
            await asyncio.sleep(5)
    
    # Final statistics
    duration = time.time() - start_time
    
    print(f"\n🎉 MacBook Pro async classification completed!")
    print(f"⏱️ Duration: {duration/60:.1f} minutes")
    print(f"✅ Successful classifications: {successful_classifications:,}/{total_classifications:,}")
    print(f"📊 Success rate: {(successful_classifications/total_classifications*100):.1f}%")
    print(f"⚡ Speed: {total_classifications/(duration/60):.1f} classifications/minute")
    
    # Check MongoDB final count
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    total_classified = db["classified_jobs"].count_documents({})
    
    print(f"\n📈 MongoDB Status:")
    print(f"   Total classified jobs: {total_classified:,}")
    print(f"🚀 Next: python simple_index_jobs.py")
    
    client.close()

if __name__ == "__main__":
    # MacBook Pro performance optimizations
    if platform.system() == "Darwin":  # macOS
        # Optimize for macOS AI workloads
        import resource
        try:
            # Optimize memory and file limits
            resource.setrlimit(resource.RLIMIT_NOFILE, (8192, 8192))
            
            # Apple Silicon optimizations
            if IS_APPLE_SILICON:
                # Enable Apple Silicon neural engine optimizations
                os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
                os.environ['MKL_NUM_THREADS'] = str(min(CPU_CORES, 8))
        except:
            pass
    
    # Run the async main function
    asyncio.run(main()) 