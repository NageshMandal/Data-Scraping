#!/usr/bin/env python3
"""
Server-Optimized AI Classification
=================================

Ultra high-performance AI classification for 92GB RAM server:
- 8 concurrent API calls
- 4 req/sec Groq rate limiting
- 8GB memory buffer
- 25 jobs/batch processing
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
import psutil

# Load environment variables
load_dotenv()

# Load server configuration
def load_server_config():
    config_path = os.getenv('SERVER_CONFIG_PATH', 'config/server_config.json')
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            return config["performance_config"]["ai_classification"]
    except:
        return {
            "max_workers": 8,
            "rate_limit_per_second": 4,
            "batch_size": 25,
            "max_retries": 3,
            "timeout": 90,
            "memory_buffer_mb": 8192
        }

# ——— Server Performance Configuration ———
server_config = load_server_config()
MAX_WORKERS = server_config["max_workers"]
GROQ_RATE_LIMIT_PER_SECOND = server_config["rate_limit_per_second"]
BATCH_SIZE = server_config["batch_size"]
MAX_RETRIES = server_config["max_retries"]
TIMEOUT = server_config["timeout"]

print(f"🔥 SERVER-OPTIMIZED AI CLASSIFICATION")
print(f"   Workers: {MAX_WORKERS} (Enhanced for server)")
print(f"   Groq Rate: {GROQ_RATE_LIMIT_PER_SECOND} req/sec")
print(f"   Batch Size: {BATCH_SIZE}")

# ——— Configuration ———
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    print("❌ ERROR: MONGO_URI not found in environment variables!")
    exit(1)
DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")
SOURCE_COLLECTION = "job_data"  # Server uses job_data collection
DEST_COLLECTION = "classified_jobs"

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise RuntimeError("GROQ_API_KEY not found in .env file")

# ——— Server-Enhanced Rate Limiter ———
class ServerAPIRateLimiter:
    def __init__(self, max_calls_per_second):
        self.max_calls = max_calls_per_second
        self.calls = []
        self.lock = Lock()
        self.adaptive_factor = 1.0
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            # Server load adaptation
            memory_usage = psutil.virtual_memory().percent
            if memory_usage > 85:
                self.adaptive_factor = 0.7  # Slow down if memory high
            elif memory_usage < 60:
                self.adaptive_factor = 1.2  # Speed up if memory low
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

groq_rate_limiter = ServerAPIRateLimiter(GROQ_RATE_LIMIT_PER_SECOND)

# ——— Server Statistics Tracking ———
class ServerClassificationStatsTracker:
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
            if len(self.memory_samples) > 50:
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

stats = ServerClassificationStatsTracker()

# ——— MongoDB with server optimization ———
thread_local = threading.local()

def get_mongo_connections():
    """Get thread-local MongoDB connections with server optimization"""
    if not hasattr(thread_local, 'client'):
        thread_local.client = MongoClient(
            MONGO_URI,
            maxPoolSize=200,  # Server-grade pool
            minPoolSize=100,
            socketTimeoutMS=240000,
            connectTimeoutMS=60000
        )
        thread_local.db = thread_local.client[DB_NAME]
        thread_local.source_col = thread_local.db[SOURCE_COLLECTION]
        thread_local.dest_col = thread_local.db[DEST_COLLECTION]
        
    return thread_local.source_col, thread_local.dest_col

def get_groq_client():
    """Get thread-local Groq client"""
    if not hasattr(thread_local, 'groq'):
        thread_local.groq = AsyncGroq(api_key=GROQ_API_KEY)
    return thread_local.groq

# ——— Enhanced LLM Classification Function (SAME PROMPTS AS ORIGINAL) ———
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def classify_job_post_server_optimized(job_data):
    groq_rate_limiter.wait_if_needed()
    
    job_data_copy = job_data.copy()
    job_data_copy.pop('_id', None)
    job_json = json.dumps(job_data_copy, indent=2)

    # EXACT SAME PROMPT AS ORIGINAL
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

JOB POST DATA:
{job_json}

JSON OUTPUT:
    """

    client = get_groq_client()
    
    response = await client.chat.completions.create(
        model="qwen2.5-72b-instruct",  # Enhanced model for server
        messages=[
            {"role": "system", "content": "You are a helpful assistant that returns only valid JSON objects for job post classification."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.1
    )

    raw_output = response.choices[0].message.content.strip()
    
    try:
        data = json.loads(raw_output)
        return data
    except json.JSONDecodeError:
        try:
            import re
            json_match = re.search(r'\{.*\}', raw_output, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                data = json.loads(json_str)
                return data
            else:
                raise ValueError("No JSON object found in response")
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON output")

# ——— MongoDB Save Function (SAME AS ORIGINAL) ———
def save_to_mongo_server_optimized(document, source_url=None):
    source_col, dest_col = get_mongo_connections()
    
    if source_url:
        document["source_url"] = source_url
    document["classified_at"] = time.time()
    document["server_processed"] = True  # Mark as server-processed
    
    try:
        result = dest_col.insert_one(document)
        return True
    except Exception as e:
        print(f"❌ MongoDB save error: {e}")
        return False

def classify_job_worker_server_optimized(job_doc: Dict[Any, Any]) -> tuple[bool, str]:
    """Server-optimized worker function"""
    try:
        source_col, dest_col = get_mongo_connections()
        
        source_url = job_doc.get('source_url', job_doc.get('url'))
        thread_id = threading.current_thread().ident
        
        # Check if already classified
        existing = dest_col.find_one({"source_url": source_url})
        if existing:
            return True, "Already classified"
        
        # Run async classification
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            classification = loop.run_until_complete(classify_job_post_server_optimized(job_doc))
            
            if isinstance(classification, dict):
                if save_to_mongo_server_optimized(classification, source_url):
                    company = classification.get('original_data', {}).get('company_name', 'Unknown')
                    position = classification.get('original_data', {}).get('position', 'Unknown')
                    stage = classification.get('classification', {}).get('company_stage', 'unknown')
                    
                    print(f"🤖 Server-{thread_id % 1000:03d} | {company} - {position} ({stage})")
                    return True, "Success"
                else:
                    return False, "MongoDB save failed"
            else:
                return False, f"Classification failed: {classification}"
                
        finally:
            loop.close()
            
    except Exception as e:
        return False, f"Worker error: {str(e)}"

def server_progress_reporter():
    """Enhanced progress reporter for server AI classification"""
    while True:
        time.sleep(60)
        stats.sample_resources()
        stats_data = stats.get_stats()
        
        try:
            source_col, dest_col = get_mongo_connections()
            total_jobs = source_col.count_documents({})
            classified_count = dest_col.count_documents({})
            remaining = total_jobs - classified_count
        except:
            total_jobs = classified_count = remaining = 0
        
        print(f"\n🤖 SERVER AI CLASSIFICATION PROGRESS:")
        print(f"   Processed: {stats_data['processed']:,}")
        print(f"   Success: {stats_data['success']:,} | Errors: {stats_data['errors']:,}")
        print(f"   Database: {classified_count:,} classified / {total_jobs:,} total")
        print(f"   Remaining: {remaining:,}")
        print(f"   Server rate: {stats_data['rate']:.1f} jobs/sec")
        print(f"   Server RAM: {stats_data['current_memory_gb']:.1f}GB used")
        print(f"   Elapsed: {stats_data['elapsed']/60:.1f} minutes\n")

def main():
    print(f"🤖 SERVER-OPTIMIZED AI CLASSIFICATION STARTING")
    print(f"🔥 MAXIMUM PERFORMANCE MODE")
    print(f"🖥️ Server RAM: {psutil.virtual_memory().total / (1024**3):.1f}GB")
    print("=" * 60)
    
    source_col, dest_col = get_mongo_connections()
    
    # Get already classified URLs
    classified_urls = set()
    for doc in dest_col.find({}, {"source_url": 1}):
        if "source_url" in doc:
            classified_urls.add(doc["source_url"])
    
    print(f"📊 Found {len(classified_urls):,} already classified jobs")
    
    # Only process unclassified jobs
    query = {}
    if classified_urls:
        query = {"url": {"$nin": list(classified_urls)}}
    
    cursor = source_col.find(query)
    unclassified_jobs = list(cursor)
    total_to_process = len(unclassified_jobs)
    
    print(f"📊 Total job data: {source_col.count_documents({}):,}")
    print(f"📋 Unclassified: {total_to_process:,}")
    
    if total_to_process == 0:
        print("✅ All jobs already classified!")
        return
    
    # Start server progress reporter
    progress_thread = threading.Thread(target=server_progress_reporter, daemon=True)
    progress_thread.start()
    
    completed_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Process in server-optimized batches
        batch_size = 150  # Larger batches for server
        
        for i in range(0, len(unclassified_jobs), batch_size):
            batch = unclassified_jobs[i:i + batch_size]
            print(f"🔄 Processing server batch {i//batch_size + 1}: {len(batch)} jobs")
            
            future_to_job = {
                executor.submit(classify_job_worker_server_optimized, job_doc): job_doc
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
                        
                        if completed_count % 100 == 0:
                            print(f"📈 Server progress: {completed_count:,}/{len(unclassified_jobs):,}")
                            
                    except Exception as e:
                        stats.increment_error()
                        print(f"❌ Future error: {e}")
            
            except KeyboardInterrupt:
                print("\n🛑 Server interrupted")
                executor.shutdown(wait=False)
                break
    
    # Final server statistics
    stats_data = stats.get_stats()
    final_classified = dest_col.count_documents({})
    final_remaining = source_col.count_documents({}) - final_classified
    
    print(f"\n" + "=" * 60)
    print(f"🤖 SERVER-OPTIMIZED AI CLASSIFICATION COMPLETE!")
    print(f"📊 Final Server Statistics:")
    print(f"   Jobs processed: {stats_data['processed']:,}")
    print(f"   Successful: {stats_data['success']:,}")
    print(f"   Errors: {stats_data['errors']:,}")
    print(f"   Total time: {stats_data['elapsed']/60:.1f} minutes")
    print(f"   Server rate: {stats_data['rate']:.1f} jobs/sec")
    print(f"   Peak memory: {max(stats.memory_samples) if stats.memory_samples else 0:.1f}GB")
    print(f"\n📈 Database Results:")
    print(f"   Jobs classified: {final_classified:,}")
    print(f"   Remaining: {final_remaining:,}")
    print(f"🚀 Next: python main_server_optimized.py --step index_data")

if __name__ == "__main__":
    def signal_handler(sig, frame):
        print('\n🛑 Server graceful shutdown...')
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    main() 