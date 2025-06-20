#!/usr/bin/env python3
"""
Simple Job Indexing to Elasticsearch
===================================

Index classified jobs using auto-mapping (which works perfectly).
Bypasses complex mapping that was causing failures.
"""

import os
import time
from pymongo import MongoClient
from elasticsearch_requests import ElasticsearchRequests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def index_classified_jobs_simple():
    """Index classified jobs using auto-mapping approach"""
    
    print("🚀 Simple Indexing: Classified Jobs to Elasticsearch")
    print("=" * 55)
    
    # Connect to MongoDB
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")
    
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    classified_jobs = db["classified_jobs"]
    
    # Connect to Elasticsearch
    es = ElasticsearchRequests()
    
    # Test connection
    print("🔗 Testing Elasticsearch connection...")
    if not es.ping():
        print("❌ Elasticsearch connection failed!")
        return False
    
    health = es.cluster_health()
    print(f"✅ Connected to {health['cluster_name']} ({health['status']} status)")
    
    # Get all classified jobs
    print(f"\n📊 Fetching classified jobs from MongoDB...")
    total_jobs = classified_jobs.count_documents({})
    print(f"Found {total_jobs} classified jobs to index")
    
    if total_jobs == 0:
        print("⚠️ No classified jobs found in MongoDB!")
        return False
    
    # Process jobs with simplified structure
    processed = 0
    success_count = 0
    error_count = 0
    
    print(f"\n🔄 Starting indexing (auto-mapping will create index)...")
    
    cursor = classified_jobs.find({})
    
    for job_doc in cursor:
        processed += 1
        
        try:
            # Extract and flatten data for easier searching
            original_data = job_doc.get('original_data', {})
            classification = job_doc.get('classification', {})
            prospecting_intel = job_doc.get('prospecting_intel', {})
            
            # Create a flat, searchable document
            company_name = original_data.get('company_name', 'unknown_company')
            safe_company_name = company_name.lower().replace(' ', '_').replace('&', 'and').replace('.', '').replace('/', '_')
            
            # Simplified document structure (ES will auto-map)
            doc = {
                # Company info
                "company_name": company_name,
                "company_domain": prospecting_intel.get('company_domain'),
                "company_industries": original_data.get('company_industries', []),
                "company_size": original_data.get('company_size'),
                "company_funding": original_data.get('amount_raised'),
                "company_founder": original_data.get('founder'),
                "company_location": original_data.get('company_location'),
                
                # Job info
                "job_position": original_data.get('position'),
                "job_location": original_data.get('location'),
                "job_salary": original_data.get('price'),
                "job_description": original_data.get('job_description', '')[:500],  # Truncated
                "job_skills": original_data.get('skills', []),
                "job_remote_policy": original_data.get('remote_work_pol'),
                "job_visa": original_data.get('visa'),
                
                # Classification
                "categories": classification.get('primary_categories', []),
                "company_stage": classification.get('company_stage'),
                "hiring_urgency": classification.get('hiring_urgency'),
                "focus_technical": classification.get('focus_areas', {}).get('technical', []),
                "focus_business": classification.get('focus_areas', {}).get('business', []),
                
                # Prospecting intel
                "investment_readiness": prospecting_intel.get('investment_readiness'),
                "key_technologies": prospecting_intel.get('key_technologies', []),
                "contact_potential": prospecting_intel.get('contact_potential'),
                "hiring_volume": prospecting_intel.get('hiring_volume'),
                
                # Metadata
                "keywords": job_doc.get('keywords', []),
                "summary": job_doc.get('summary'),
                "source_url": job_doc.get('source_url'),
                "indexed_at": time.time(),
                "classification_date": job_doc.get('classified_at')
            }
            
            # Use job URL as unique ID (more granular than company name)
            source_url = job_doc.get('source_url', f"job_{processed}")
            doc_id = source_url.split('/')[-1] if '/' in source_url else f"job_{processed}"
            
            # Index the document (ES will auto-create index on first document)
            response = es._request("PUT", f"/{es.index}/_doc/{doc_id}", doc)
            
            if response and response.status_code in [200, 201]:
                success_count += 1
                if processed <= 5:  # Show first few for progress
                    print(f"✅ [{processed}/{total_jobs}] Indexed: {company_name} - {original_data.get('position', 'Unknown Position')}")
                elif processed % 50 == 0:  # Then show every 50
                    print(f"📊 [{processed}/{total_jobs}] Progress: {success_count} successful, {error_count} errors")
            else:
                error_count += 1
                if error_count <= 3:  # Show first few errors
                    print(f"❌ [{processed}/{total_jobs}] Failed: {company_name} - {response.status_code if response else 'No response'}")
                
        except Exception as e:
            error_count += 1
            if error_count <= 3:
                print(f"❌ [{processed}/{total_jobs}] Error: {e}")
    
    # Final statistics
    print(f"\n" + "=" * 55)
    print(f"🎉 Indexing completed!")
    print(f"📊 Final Statistics:")
    print(f"   Total jobs processed: {processed}")
    print(f"   Successfully indexed: {success_count}")
    print(f"   Errors: {error_count}")
    print(f"   Success rate: {(success_count/processed*100):.1f}%" if processed > 0 else "N/A")
    
    # Check final index stats
    try:
        stats_response = es._request("GET", f"/{es.index}/_stats")
        if stats_response and stats_response.status_code == 200:
            stats_data = stats_response.json()
            doc_count = stats_data["indices"][es.index]["total"]["docs"]["count"]
            size_bytes = stats_data["indices"][es.index]["total"]["store"]["size_in_bytes"]
            
            print(f"\n📈 Elasticsearch Index Stats:")
            print(f"   Documents in index: {doc_count}")
            print(f"   Index size: {size_bytes:,} bytes ({size_bytes/1024/1024:.1f} MB)")
        else:
            print(f"\n⚠️ Could not retrieve index stats")
    except Exception as e:
        print(f"\n⚠️ Error getting stats: {e}")
    
    mongo_client.close()
    return success_count > 0

if __name__ == "__main__":
    success = index_classified_jobs_simple()
    
    if success:
        print(f"\n🚀 SUCCESS! Your classified jobs are now searchable in Elasticsearch!")
        print(f"🔍 You can search by:")
        print(f"   • Company name, industry, funding stage")
        print(f"   • Job title, location, salary, skills")
        print(f"   • Technologies, investment readiness")
        print(f"   • Categories, hiring urgency, contact potential")
    else:
        print(f"\n❌ Indexing failed. Check the errors above.") 