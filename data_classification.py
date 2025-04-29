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
ES_INDEX = "job_classifications"

openai.api_key = "YOUR_OPENAI_API_KEY"

# ——— Setup Clients ———
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
source_col = db[SOURCE_COLLECTION]
dest_col = db[DEST_COLLECTION]

es = Elasticsearch(ES_HOST)

# ——— LLM Prompt Function ———
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def classify_job_post(job_data):
    # Remove MongoDB _id field before sending to GPT
    job_data.pop('_id', None)

    job_json = json.dumps(job_data, indent=2)
    
    prompt = f"""
You are a job classification assistant. Given a detailed job post, classify the following:

1. Categories (only based on real company focus).
2. Focus areas for each category (Focus 1, Focus 2).
3. Company domain if known.
4. Job info (title, location, type, salary, etc.).
5. A summary of hiring and product intent.
6. Product investment signals.
7. Output strictly in valid JSON.

Here is the job post data:
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
    doc_id = company_name

    if not company_name:
        print("⚠️ No company name found, skipping Elasticsearch update.")
        return

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

# ——— Batch Processing Function ———
async def process_jobs(batch_size=5):
    cursor = source_col.find({})

    batch = []
    for job in cursor:  # Note: not async for
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
