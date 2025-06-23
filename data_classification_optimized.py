#!/usr/bin/env python3
"""
Optimized AI Classification with Concurrency
============================================

High-performance job classification using concurrent Groq API calls.
Uses EXACT same prompts, models, and data structure as original.
Optimized for MacBook M1 Pro with careful API rate limiting.
"""

import os
import json
import time
import asyncio
from typing import List, Dict, Any
from pymongo import MongoClient
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import threading
import signal
import sys
from groq import AsyncGroq
from tenacity import retry, wait_exponential, stop_after_attempt
import ssl
import urllib3

# Load environment variables
load_dotenv()

# ——— Performance Configuration for M1 Pro ———
MAX_WORKERS = 4  # Conservative for API calls (Groq has rate limits)
GROQ_RATE_LIMIT_PER_SECOND = 2  # Groq free tier limit
BATCH_SIZE = 5  # Same as original for consistency
MAX_RETRIES = 3
TIMEOUT = 60

# ——— Configurations (EXACT SAME AS ORIGINAL) ———
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("❌ ERROR: MONGO_URI not found in environment variables!")
    exit(1)
DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")
SOURCE_COLLECTION = "jobs"  # Same as original
DEST_COLLECTION = "classified_jobs"  # Same as original

# Set Groq API key (EXACT SAME AS ORIGINAL)
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise RuntimeError("GROQ_API_KEY not found in .env file")

# ——— Global Rate Limiter ———
class APIRateLimiter:
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

groq_rate_limiter = APIRateLimiter(GROQ_RATE_LIMIT_PER_SECOND)

# ——— Statistics Tracking ———
class ClassificationStatsTracker:
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

stats = ClassificationStatsTracker()

# ——— MongoDB setup ———
# Thread-local connections
thread_local = threading.local()

def get_mongo_connections():
    """Get thread-local MongoDB connections"""
    if not hasattr(thread_local, 'client'):
        thread_local.client = MongoClient(MONGO_URI)
        thread_local.db = thread_local.client[DB_NAME]
        thread_local.source_col = thread_local.db[SOURCE_COLLECTION]
        thread_local.dest_col = thread_local.db[DEST_COLLECTION]
        
    return thread_local.source_col, thread_local.dest_col

def get_groq_client():
    """Get thread-local Groq client"""
    if not hasattr(thread_local, 'groq'):
        thread_local.groq = AsyncGroq(api_key=GROQ_API_KEY)
    return thread_local.groq

# ——— LLM Prompt Function (EXACT SAME AS ORIGINAL) ———
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def classify_job_post(job_data):
    groq_rate_limiter.wait_if_needed()
    
    job_data_copy = job_data.copy()
    job_data_copy.pop('_id', None)
    job_json = json.dumps(job_data_copy, indent=2)

    prompt = f"""You are a job classification assistant. Analyze the detailed job post data and return ONLY a valid JSON object with the structure shown below.

The input data contains rich information already extracted from the job posting. Use this information intelligently to create a comprehensive classification.

REQUIRED JSON OUTPUT STRUCTURE:
{{
  "original_data": {{
    "company_name": "Extract from input",
    "position": "Extract from input", 
    "location": "Extract from input",
    "price": "Extract from input",
    "job_description": "Extract from input (first 500 chars)",
    "company_industries": "Extract from input array",
    "company_size": "Extract from input",
    "amount_raised": "Extract from input",
    "founder": "Extract from input",
    "hiring_stat": "Extract from input",
    "skills": "Extract from input array",
    "perks": "Extract from input array (first 3)",
    "remote_work_pol": "Extract from input",
    "visa": "Extract from input"
  }},
  "classification": {{
    "primary_categories": ["category1", "category2"],
    "focus_areas": {{
      "technical": ["focus1", "focus2"],
      "business": ["focus1", "focus2"]
    }},
    "company_stage": "Extract/infer from funding data",
    "hiring_urgency": "Low/Medium/High based on hiring_stat",
    "investment_signals": {{
      "funding_status": "Based on amount_raised",
      "growth_indicators": ["indicator1", "indicator2"],
      "market_position": "Based on company info"
    }},
    "role_analysis": {{
      "seniority_level": "Extract from position title",
      "department": "Engineering/Product/Data/etc",
      "remote_friendly": "Yes/No/Hybrid based on location/policy"
    }}
  }},
  "prospecting_intel": {{
    "company_domain": "Infer website domain from company name",
    "key_technologies": ["tech1", "tech2", "tech3"],
    "hiring_volume": "Based on company size and growth stage",
    "contact_potential": "High/Medium/Low",
    "investment_readiness": "Based on funding stage and growth signals"
  }},
  "keywords": ["keyword1", "keyword2", "keyword3"],
  "summary": "One sentence summary of why this company/role is relevant for prospecting"
}}

CLASSIFICATION GUIDELINES:
- Primary categories should be specific (e.g., "AI/ML Platform", "Developer Tools", "Fintech", "Healthcare Tech")
- Use the company_industries array to inform categorization
- Analyze job_description for technical focus areas
- Consider company_size, amount_raised, and hiring_stat for growth signals
- Extract key technologies from job_description and skills
- Infer company domain as companyname.com (lowercase, no spaces/special chars)

JOB POST DATA:
{job_json}

JSON OUTPUT:
    """

    client = get_groq_client()
    
    response = await client.chat.completions.create(
        model="qwen/qwen3-32b",  # EXACT SAME MODEL AS ORIGINAL
        messages=[
            {"role": "system", "content": "You are a helpful assistant that returns only valid JSON objects for job post classification."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.1  # EXACT SAME TEMPERATURE AS ORIGINAL
    )

    raw_output = response.choices[0].message.content.strip()
    
    try:
        # First try direct JSON parsing
        data = json.loads(raw_output)
        return data
    except json.JSONDecodeError:
        # Try to extract JSON from the response if it's wrapped in text
        try:
            # Look for JSON object in the response
            import re
            json_match = re.search(r'\{.*\}', raw_output, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                data = json.loads(json_str)
                return data
            else:
                print(f"⚠️ No JSON object found in response: {raw_output[:200]}...")
                raise ValueError("No JSON object found in response")
        except json.JSONDecodeError:
            print(f"⚠️ Invalid JSON returned even after extraction: {raw_output[:200]}...")
            raise ValueError("Invalid JSON output")

# ——— MongoDB Save Function (EXACT SAME AS ORIGINAL) ———
def save_to_mongo(document, source_url=None):
    source_col, dest_col = get_mongo_connections()
    
    # Add metadata to the document
    if source_url:
        document["source_url"] = source_url
    document["classified_at"] = time.time()
    
    try:
        result = dest_col.insert_one(document)
        print(f"✅ Classified job saved to MongoDB (ID: {result.inserted_id})")
        return True
    except Exception as e:
        print(f"❌ MongoDB save error: {e}")
        return False

def classify_job_worker(job_doc: Dict[Any, Any]) -> tuple[bool, str]:
    """Worker function to classify a single job using async processing"""
    try:
        source_col, dest_col = get_mongo_connections()
        
        source_url = job_doc.get('source_url', job_doc.get('url'))
        thread_id = threading.current_thread().ident
        
        # Check if already classified
        existing = dest_col.find_one({"source_url": source_url})
        if existing:
            return True, "Already classified"
        
        # Run the async classification in this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Classify the job using the original function
            classification = loop.run_until_complete(classify_job_post(job_doc))
            
            if isinstance(classification, dict):
                # Save to MongoDB with source URL tracking (EXACT SAME AS ORIGINAL)
                if save_to_mongo(classification, source_url):
                    # Log progress
                    company = classification.get('original_data', {}).get('company_name', 'Unknown')
                    position = classification.get('original_data', {}).get('position', 'Unknown')
                    stage = classification.get('classification', {}).get('company_stage', 'unknown')
                    
                    print(f"🤖 Thread-{thread_id % 1000:03d} | {company} - {position} ({stage})")
                    return True, "Success"
                else:
                    return False, "MongoDB save failed"
            else:
                return False, f"Classification failed: {classification}"
                
        finally:
            loop.close()
            
    except Exception as e:
        return False, f"Worker error: {str(e)}"

def progress_reporter():
    """Background thread to report classification progress"""
    while True:
        time.sleep(60)  # Report every minute
        stats_data = stats.get_stats()
        
        try:
            source_col, dest_col = get_mongo_connections()
            total_jobs = source_col.count_documents({})
            classified_count = dest_col.count_documents({})
            remaining = total_jobs - classified_count
        except:
            total_jobs = classified_count = remaining = 0
        
        print(f"\n🤖 AI Classification Progress:")
        print(f"   Processed: {stats_data['processed']}")
        print(f"   Success: {stats_data['success']} | Errors: {stats_data['errors']}")
        print(f"   Database: {classified_count:,} classified / {total_jobs:,} total")
        print(f"   Remaining: {remaining:,}")
        print(f"   Rate: {stats_data['rate']:.1f} jobs/sec")
        print(f"   Elapsed: {stats_data['elapsed']/60:.1f} minutes\n")

# ——— Main optimized processing (USING ORIGINAL LOGIC) ———
def main():
    print(f"🤖 Optimized AI Classification Starting")
    print(f"📋 Using EXACT same prompts/model as original")
    print(f"🔧 Max Workers: {MAX_WORKERS}")
    print(f"⚡ Groq Rate Limit: {GROQ_RATE_LIMIT_PER_SECOND} req/sec")
    print("=" * 50)
    
    # Get database connections
    source_col, dest_col = get_mongo_connections()
    
    # Get list of already classified job URLs to avoid reprocessing (EXACT SAME AS ORIGINAL)
    classified_urls = set()
    for doc in dest_col.find({}, {"source_url": 1}):
        if "source_url" in doc:
            classified_urls.add(doc["source_url"])
    
    print(f"📊 Found {len(classified_urls)} already classified jobs")
    
    # Only process jobs that haven't been classified yet (EXACT SAME AS ORIGINAL)
    query = {}
    if classified_urls:
        query = {"source_url": {"$nin": list(classified_urls)}}
    
    cursor = source_col.find(query)
    unclassified_jobs = list(cursor)
    total_to_process = len(unclassified_jobs)
    
    print(f"📊 Total job data: {source_col.count_documents({})}")
    print(f"📋 Unclassified: {total_to_process:,}")
    
    if total_to_process == 0:
        print("✅ All jobs have already been classified!")
        return
    
    # Start progress reporter
    progress_thread = threading.Thread(target=progress_reporter, daemon=True)
    progress_thread.start()
    
    # Process jobs concurrently
    completed_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit jobs in batches
        batch_size = 100
        
        for i in range(0, len(unclassified_jobs), batch_size):
            batch = unclassified_jobs[i:i + batch_size]
            print(f"🔄 Processing batch {i//batch_size + 1}: {len(batch)} jobs")
            
            # Submit batch
            future_to_job = {
                executor.submit(classify_job_worker, job_doc): job_doc
                for job_doc in batch
            }
            
            try:
                for future in as_completed(future_to_job):
                    job_doc = future_to_job[future]
                    stats.increment_processed()
                    
                    try:
                        success, message = future.result()
                        if success:
                            stats.increment_success()
                        else:
                            stats.increment_error()
                            
                        completed_count += 1
                        
                        # Progress indicator
                        if completed_count % 50 == 0:
                            print(f"📈 Progress: {completed_count}/{len(unclassified_jobs)}")
                            
                    except Exception as e:
                        stats.increment_error()
                        print(f"❌ Future error: {e}")
            
            except KeyboardInterrupt:
                print("\n🛑 Interrupted by user")
                executor.shutdown(wait=False)
                break
    
    # Final statistics
    stats_data = stats.get_stats()
    final_classified = dest_col.count_documents({})
    final_remaining = source_col.count_documents({}) - final_classified
    
    print(f"\n" + "=" * 50)
    print(f"🤖 AI Classification Complete!")
    print(f"📊 Final Statistics:")
    print(f"   Jobs processed: {stats_data['processed']:,}")
    print(f"   Successful: {stats_data['success']:,}")
    print(f"   Errors: {stats_data['errors']:,}")
    print(f"   Total time: {stats_data['elapsed']/60:.1f} minutes")
    print(f"   Average rate: {stats_data['rate']:.1f} jobs/sec")
    print(f"\n📈 Database Stats:")
    print(f"   Jobs classified: {final_classified:,}")
    print(f"   Remaining: {final_remaining:,}")
    print(f"🚀 Next step: python main.py --step index_data")

if __name__ == "__main__":
    def signal_handler(sig, frame):
        print('\n🛑 Graceful shutdown...')
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    main()

    print("🤖 Optimized Classification Script Created!")
    print("This is the optimized version with concurrency support.") 