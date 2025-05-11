import asyncio
import openai
import json
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from tenacity import retry, wait_exponential, stop_after_attempt

# ——— Configurations ———
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "job_scraping"
SOURCE_COLLECTION = "jobs"
DEST_COLLECTION = "classified_jobs"

ES_HOST = "http://localhost:9200"
ES_INDEX = "project_jobposters_index"
ES_USER = "project_jobposters_user"
ES_PASS = "project_jobposters_1234"

openai.api_key = "YOUR_OPENAI_API_KEY"

# ——— Setup Clients ———
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
source_col = db[SOURCE_COLLECTION]
dest_col = db[DEST_COLLECTION]

es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USER, ES_PASS)
)

# ——— LLM Prompt Function ———
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def classify_job_post(job_data):
    job_data.pop('_id', None)
    job_json = json.dumps(job_data, indent=2)

    prompt = f"""
You are a job classification assistant. Given a detailed job post, classify the following:

1. Categories (based on what the company is working on or investing in, not general terms like CRM unless explicitly mentioned).
2. Focus areas for each category (2 specific focus points).
3. Company domain and basic info.
4. Job info (title, location, type, salary, posting age).
5. A summary of intent.
6. Any signals for product investment or hiring priority.
7. Contact person info (if provided).
8. Output in JSON.

Only use categories that align with the company’s real focus based on the job post content.

Here is the job post:
{job_json}
    """

    response = await openai.ChatCompletion.acreate(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant for job post classification."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.2
    )

    raw_output = response['choices'][0]['message']['content']
    
    try:
        data = json.loads(raw_output)
        return data
    except json.JSONDecodeError:
        print("⚠️ Invalid JSON returned. Retry...")
        raise ValueError("Invalid JSON output")

# ——— MongoDB Save Function ———
def save_to_mongo(document):
    dest_col.insert_one(document)

# ——— Elasticsearch Update/Insert Function ———
def upsert_to_elasticsearch(data):
    company_info = data.get('company', {})
    company_name = company_info.get('name', '').lower().replace(" ", "_")
    doc_id = company_name or None

    if not doc_id:
        print("⚠️ No company name found, skipping Elasticsearch update.")
        return

    try:
        if es.exists(index=ES_INDEX, id=doc_id):
            existing_doc = es.get(index=ES_INDEX, id=doc_id)['_source']
            jobs = existing_doc.get("jobs", [])
            jobs.append(data['job'])

            updated_doc = {
                **existing_doc,
                "jobs": jobs,
                "latest_update": data
            }
            es.index(index=ES_INDEX, id=doc_id, body=updated_doc)
        else:
            new_doc = {
                "company": data["company"],
                "jobs": [data["job"]],
                "categories": data["categories"],
                "focus": data["focus"],
                "intent_summary": data["intent_summary"],
                "signals": data["relevant_for_prospecting"],
                "latest_update": data
            }
            es.index(index=ES_INDEX, id=doc_id, body=new_doc)
    except Exception as e:
        print(f"❌ Elasticsearch error: {e}")

# ——— Batch Processing Function ———
async def process_jobs(batch_size=5):
    cursor = source_col.find({})
    batch = []

    for job in cursor:
        batch.append(job)
        if len(batch) >= batch_size:
            await process_batch(batch)
            batch = []

    if batch:
        await process_batch(batch)

async def process_batch(batch):
    tasks = [classify_job_post(job) for job in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, dict):
            save_to_mongo(result)
            upsert_to_elasticsearch(result)
        else:
            print(f"❌ Skipping a failed job classification: {result}")

# ——— Run it ———
if __name__ == "__main__":
    asyncio.run(process_jobs(batch_size=5))
