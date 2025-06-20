#!/usr/bin/env python3
"""
Index Classified Jobs to Elasticsearch
=====================================

Transfer all classified jobs from MongoDB to Elasticsearch
using the working requests interface.
"""

import os
import time
from datetime import datetime
from pymongo import MongoClient
from elasticsearch_requests import ElasticsearchRequests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def transfer_classified_jobs():
    """Transfer classified jobs from MongoDB to Elasticsearch"""
    
    print("🚀 Indexing Classified Jobs to Elasticsearch")
    print("=" * 50)
    
    # Connect to MongoDB
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        print("❌ ERROR: MONGO_URI not found in environment variables!")
        exit(1)
    DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")
    
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    classified_jobs = db["classified_jobs"]
    
    # Connect to Elasticsearch
    es = ElasticsearchRequests()
    
    # Test ES connection
    print("🔗 Testing Elasticsearch connection...")
    if not es.ping():
        print("❌ Elasticsearch connection failed!")
        return False
    
    health = es.cluster_health()
    print(f"✅ Connected to {health['cluster_name']} ({health['status']} status)")
    
    # Create index if it doesn't exist
    print(f"\n📂 Setting up index '{es.index}'...")
    if not es.index_exists():
        print("Creating index with job data mapping...")
        if es.create_index():
            print("✅ Index created successfully!")
        else:
            print("❌ Failed to create index!")
            return False
    else:
        print("✅ Index already exists!")
    
    # Get all classified jobs
    print(f"\n📊 Fetching classified jobs from MongoDB...")
    total_jobs = classified_jobs.count_documents({})
    print(f"Found {total_jobs} classified jobs to index")
    
    if total_jobs == 0:
        print("⚠️ No classified jobs found in MongoDB!")
        return False
    
    # Process jobs in batches
    batch_size = 10
    processed = 0
    success_count = 0
    error_count = 0
    
    print(f"\n🔄 Processing in batches of {batch_size}...")
    
    cursor = classified_jobs.find({})
    
    for job_doc in cursor:
        processed += 1
        
        try:
            # Extract data for Elasticsearch
            original_data = job_doc.get('original_data', {})
            classification = job_doc.get('classification', {})
            prospecting_intel = job_doc.get('prospecting_intel', {})
            
            # Create company name for document ID
            company_name = original_data.get('company_name', 'unknown_company')
            safe_company_name = company_name.lower().replace(' ', '_').replace('&', 'and').replace('.', '').replace('/', '_')
            
            # Check if company document already exists
            existing_doc = es.get_document(safe_company_name)
            
            if existing_doc and existing_doc.get('found'):
                # Update existing company with new job
                company_data = existing_doc['_source']
                jobs = company_data.get('jobs', [])
                
                # Add new job to the jobs array
                new_job = {
                    "position": original_data.get('position'),
                    "location": original_data.get('location'),
                    "salary": original_data.get('price'),
                    "department": classification.get('role_analysis', {}).get('department'),
                    "seniority": classification.get('role_analysis', {}).get('seniority_level'),
                    "remote_friendly": classification.get('role_analysis', {}).get('remote_friendly'),
                    "skills": original_data.get('skills', []),
                    "source_url": job_doc.get('source_url'),
                    "classification_date": job_doc.get('classified_at', time.time())
                }
                jobs.append(new_job)
                
                # Update the document
                company_data['jobs'] = jobs
                company_data['total_jobs'] = len(jobs)
                company_data['last_updated'] = time.time()
                
                doc_id = safe_company_name
                
            else:
                # Create new company document
                company_data = {
                    "company": {
                        "name": original_data.get('company_name'),
                        "domain": prospecting_intel.get('company_domain'),
                        "industries": original_data.get('company_industries', []),
                        "size": original_data.get('company_size'),
                        "funding": original_data.get('amount_raised'),
                        "founder": original_data.get('founder'),
                        "hiring_status": original_data.get('hiring_stat'),
                        "location": original_data.get('company_location')
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
                        "source_url": job_doc.get('source_url'),
                        "classification_date": job_doc.get('classified_at', time.time())
                    }],
                    "total_jobs": 1,
                    "keywords": job_doc.get('keywords', []),
                    "summary": job_doc.get('summary'),
                    "created_at": time.time(),
                    "last_updated": time.time()
                }
                
                doc_id = safe_company_name
            
            # Index the document
            if es.index_document(doc_id, company_data):
                success_count += 1
                print(f"✅ [{processed}/{total_jobs}] Indexed: {original_data.get('company_name', 'Unknown')}")
            else:
                error_count += 1
                print(f"❌ [{processed}/{total_jobs}] Failed: {original_data.get('company_name', 'Unknown')}")
            
        except Exception as e:
            error_count += 1
            print(f"❌ [{processed}/{total_jobs}] Error processing job: {e}")
        
        # Progress update every 25 jobs
        if processed % 25 == 0:
            print(f"📊 Progress: {processed}/{total_jobs} processed, {success_count} successful, {error_count} errors")
    
    # Final statistics
    print(f"\n" + "=" * 50)
    print(f"🎉 Indexing completed!")
    print(f"📊 Final Statistics:")
    print(f"   Total jobs processed: {processed}")
    print(f"   Successfully indexed: {success_count}")
    print(f"   Errors: {error_count}")
    print(f"   Success rate: {(success_count/processed*100):.1f}%" if processed > 0 else "N/A")
    
    # Check final index stats
    stats = es.index_stats()
    if stats:
        print(f"\n📈 Elasticsearch Index Stats:")
        print(f"   Documents in index: {stats['doc_count']}")
        print(f"   Index size: {stats['size']:,} bytes")
    
    mongo_client.close()
    return success_count > 0

if __name__ == "__main__":
    success = transfer_classified_jobs()
    
    if success:
        print(f"\n🚀 Ready for searching! Your classified jobs are now in Elasticsearch.")
        print(f"🔍 You can now search by company, technology, location, funding stage, etc.")
    else:
        print(f"\n❌ Indexing failed. Check the errors above.") 