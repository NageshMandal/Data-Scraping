import json

def upsert_company_job(json_output):
    data = json.loads(json_output)
    company_name = data['company']['name'].lower().replace(" ", "_")

    # Use company_name as document ID
    doc_id = company_name

    # Check if document exists
    if es.exists(index=INDEX_NAME, id=doc_id):
        existing_doc = es.get(index=INDEX_NAME, id=doc_id)['_source']
        
        # Append job if not already present
        jobs = existing_doc.get("jobs", [])
        job_titles = [job["title"] for job in jobs]
        
        if data["job"]["title"] not in job_titles:
            jobs.append(data["job"])
        
        # Merge updated job data
        updated_doc = {
            **existing_doc,
            "jobs": jobs,
            "latest_update": data  # Optional: store latest parsed data
        }

        es.index(index=INDEX_NAME, id=doc_id, body=updated_doc)
        print(f"Updated existing company: {company_name}")
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
        es.index(index=INDEX_NAME, id=doc_id, body=new_doc)
        print(f"Indexed new company: {company_name}")
