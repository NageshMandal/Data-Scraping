import asyncio
from groq import AsyncGroq
import json
import os
import time
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from tenacity import retry, wait_exponential, stop_after_attempt
from dotenv import load_dotenv
import ssl
import urllib3

# Load environment variables
load_dotenv()

# ——— Configurations ———
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")
SOURCE_COLLECTION = "jobs"
DEST_COLLECTION = "classified_jobs"

ES_HOST = os.getenv("ES_HOST")  # Host from environment only
ES_INDEX = os.getenv("ES_INDEX", "project_jobposters_index")
ES_API_KEY = os.getenv("ES_API_KEY")  # Use API key if available
ES_USER = os.getenv("ES_USER", "elastic")  # Default username
ES_PASS = os.getenv("ES_PASS")  # Password from environment only

# Set Groq API key
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# ——— Setup Clients ———
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
source_col = db[SOURCE_COLLECTION]
dest_col = db[DEST_COLLECTION]

# Elasticsearch with SSL verification disabled for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Security check for Elasticsearch credentials
if not ES_HOST:
    print("⚠️ WARNING: ES_HOST not found in environment variables!")
    print("💡 Please set ES_HOST in your .env file")
    es = None  # Will cause errors if used, forcing proper setup
elif not ES_PASS and not ES_API_KEY:
    print("⚠️ WARNING: No Elasticsearch credentials found!")
    print("💡 Please set either ES_API_KEY or ES_PASS in your .env file")
    es = None  # Will cause errors if used, forcing proper setup
else:
    # Configure Elasticsearch client with basic auth as primary (curl testing confirmed it works)
    print("🔐 Using Elasticsearch Basic authentication")
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASS),
        verify_certs=False,  # Disable SSL certificate verification  
        ssl_show_warn=False,  # Disable SSL warnings
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=3,
        ca_certs=False,  # Additional SSL fix
        ssl_assert_hostname=False,  # Additional SSL fix
        ssl_assert_fingerprint=False  # Additional SSL fix
    )

# ——— LLM Prompt Function ———
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def classify_job_post(job_data):
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

    client = AsyncGroq(api_key=GROQ_API_KEY)
    
    response = await client.chat.completions.create(
        model="qwen/qwen3-32b",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that returns only valid JSON objects for job post classification."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.1  # Lower temperature for more consistent output
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

# ——— MongoDB Save Function ———
def save_to_mongo(document, source_url=None):
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

# ——— Elasticsearch Update/Insert Function ———
def upsert_to_elasticsearch(data):
    # Extract company name from the new structure
    original_data = data.get('original_data', {})
    classification = data.get('classification', {})
    prospecting_intel = data.get('prospecting_intel', {})
    
    company_name = original_data.get('company_name', '').lower().replace(" ", "_").replace("&", "and")
    doc_id = company_name or None

    if not doc_id:
        print("⚠️ No company name found, skipping Elasticsearch update.")
        return

    try:
        if es.exists(index=ES_INDEX, id=doc_id):
            # Update existing company document
            existing_doc = es.get(index=ES_INDEX, id=doc_id)['_source']
            jobs = existing_doc.get("jobs", [])
            
            # Add new job to the jobs array
            new_job = {
                "position": original_data.get('position'),
                "location": original_data.get('location'), 
                "salary": original_data.get('price'),
                "department": classification.get('role_analysis', {}).get('department'),
                "seniority": classification.get('role_analysis', {}).get('seniority_level'),
                "remote_friendly": classification.get('role_analysis', {}).get('remote_friendly'),
                "skills": original_data.get('skills', []),
                "classification_date": time.time()
            }
            jobs.append(new_job)

            updated_doc = {
                **existing_doc,
                "jobs": jobs,
                "total_jobs": len(jobs),
                "latest_classification": data,
                "last_updated": time.time()
            }
            es.index(index=ES_INDEX, id=doc_id, body=updated_doc)
            print(f"🔄 Updated existing company in Elasticsearch: {original_data.get('company_name')}")
            
        else:
            # Create new company document
            new_doc = {
                "company": {
                    "name": original_data.get('company_name'),
                    "domain": prospecting_intel.get('company_domain'),
                    "industries": original_data.get('company_industries', []),
                    "size": original_data.get('company_size'),
                    "funding": original_data.get('amount_raised'),
                    "founder": original_data.get('founder'),
                    "hiring_status": original_data.get('hiring_stat')
                },
                "classification": {
                    "categories": classification.get('primary_categories', []),
                    "focus_areas": classification.get('focus_areas', {}),
                    "company_stage": classification.get('company_stage'),
                    "hiring_urgency": classification.get('hiring_urgency')
                },
                "prospecting": {
                    "investment_readiness": prospecting_intel.get('investment_readiness'),
                    "key_technologies": prospecting_intel.get('key_technologies', []),
                    "contact_potential": prospecting_intel.get('contact_potential'),
                    "hiring_volume": prospecting_intel.get('hiring_volume')
                },
                "jobs": [{
                    "position": original_data.get('position'),
                    "location": original_data.get('location'),
                    "salary": original_data.get('price'), 
                    "department": classification.get('role_analysis', {}).get('department'),
                    "seniority": classification.get('role_analysis', {}).get('seniority_level'),
                    "remote_friendly": classification.get('role_analysis', {}).get('remote_friendly'),
                    "skills": original_data.get('skills', []),
                    "classification_date": time.time()
                }],
                "total_jobs": 1,
                "keywords": data.get('keywords', []),
                "summary": data.get('summary'),
                "created_at": time.time(),
                "last_updated": time.time()
            }
            es.index(index=ES_INDEX, id=doc_id, body=new_doc)
            print(f"➕ Created new company in Elasticsearch: {original_data.get('company_name')}")
            
    except Exception as e:
        print(f"❌ Elasticsearch error: {e}")
        # Don't raise the error, just log it so processing can continue

# ——— Batch Processing Function ———
async def process_jobs(batch_size=5):
    # Get list of already classified job URLs to avoid reprocessing
    classified_urls = set()
    for doc in dest_col.find({}, {"source_url": 1}):
        if "source_url" in doc:
            classified_urls.add(doc["source_url"])
    
    print(f"📊 Found {len(classified_urls)} already classified jobs")
    
    # Only process jobs that haven't been classified yet
    query = {}
    if classified_urls:
        query = {"source_url": {"$nin": list(classified_urls)}}
    
    cursor = source_col.find(query)
    total_to_process = source_col.count_documents(query)
    
    if total_to_process == 0:
        print("✅ All jobs have already been classified!")
        return
    
    print(f"📋 Processing {total_to_process} unclassified jobs...")
    
    batch = []
    processed = 0

    for job in cursor:
        batch.append(job)
        if len(batch) >= batch_size:
            await process_batch(batch)
            processed += len(batch)
            print(f"⏳ Progress: {processed}/{total_to_process} jobs processed")
            batch = []

    if batch:
        await process_batch(batch)
        processed += len(batch)
        print(f"⏳ Progress: {processed}/{total_to_process} jobs processed")

async def process_batch(batch):
    tasks = [classify_job_post(job) for job in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        job = batch[i]
        source_url = job.get("source_url", "unknown")
        
        if isinstance(result, dict):
            # Save to MongoDB with source URL tracking
            if save_to_mongo(result, source_url):
                # Only index to Elasticsearch if MongoDB save was successful
                try:
                    upsert_to_elasticsearch(result)
                    print(f"✅ Successfully processed job from: {source_url}")
                except Exception as e:
                    print(f"⚠️ Elasticsearch error for {source_url}: {e}")
            else:
                print(f"❌ Failed to save classified job from: {source_url}")
        else:
            print(f"❌ Failed to classify job from {source_url}: {result}")

# ——— Run it ———
if __name__ == "__main__":
    asyncio.run(process_jobs(batch_size=5))
