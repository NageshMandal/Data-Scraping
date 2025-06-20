#!/usr/bin/env python3
"""
Debug Index Creation
===================

Test index creation step by step to identify the issue.
"""

from elasticsearch_requests import ElasticsearchRequests
import json

def test_index_creation():
    """Test different approaches to index creation"""
    
    print("🔍 Debug Index Creation")
    print("=" * 50)
    
    es = ElasticsearchRequests()
    
    # Test connection
    if not es.ping():
        print("❌ Connection failed!")
        return
    
    print("✅ Connection successful!")
    
    # Test 1: Try to create index without mapping
    print("\n🧪 Test 1: Create index without mapping...")
    test_index = "test_simple_index"
    
    response = es._request("PUT", f"/{test_index}")
    if response and response.status_code in [200, 201]:
        print("✅ Simple index creation successful!")
        # Clean up
        es._request("DELETE", f"/{test_index}")
    else:
        print(f"❌ Simple index creation failed: {response.status_code if response else 'No response'}")
        if response:
            print(f"   Error: {response.text}")
    
    # Test 2: Create index with minimal mapping
    print("\n🧪 Test 2: Create index with minimal mapping...")
    minimal_mapping = {
        "mappings": {
            "properties": {
                "company_name": {"type": "text"},
                "position": {"type": "text"},
                "location": {"type": "keyword"}
            }
        }
    }
    
    test_index2 = "test_minimal_mapping"
    response = es._request("PUT", f"/{test_index2}", minimal_mapping)
    if response and response.status_code in [200, 201]:
        print("✅ Minimal mapping successful!")
        # Clean up
        es._request("DELETE", f"/{test_index2}")
    else:
        print(f"❌ Minimal mapping failed: {response.status_code if response else 'No response'}")
        if response:
            print(f"   Error: {response.text}")
    
    # Test 3: Check if main index already exists with different structure
    print(f"\n🧪 Test 3: Check main index '{es.index}'...")
    if es.index_exists():
        print("✅ Main index already exists!")
        
        # Get index mapping
        response = es._request("GET", f"/{es.index}/_mapping")
        if response and response.status_code == 200:
            print("   Current mapping retrieved successfully")
        else:
            print("   Could not retrieve mapping")
    else:
        print("⚠️ Main index does not exist")
        
        # Try to create with no mapping (let ES auto-create)
        print("   Trying to create with auto-mapping...")
        sample_doc = {
            "company_name": "Test Company",
            "position": "Test Position",
            "created_at": "2025-01-20T12:00:00"
        }
        
        response = es._request("PUT", f"/{es.index}/_doc/test", sample_doc)
        if response and response.status_code in [200, 201]:
            print("✅ Index auto-created with sample document!")
            
            # Clean up test document
            es._request("DELETE", f"/{es.index}/_doc/test")
        else:
            print(f"❌ Auto-creation failed: {response.status_code if response else 'No response'}")
            if response:
                print(f"   Error: {response.text}")

def try_simplified_indexing():
    """Try indexing with a simplified approach"""
    
    print("\n🚀 Trying Simplified Indexing Approach")
    print("=" * 30)
    
    es = ElasticsearchRequests()
    
    # Create a simple document without complex mapping
    sample_job = {
        "company_name": "Sample Company",
        "position": "Software Engineer",
        "location": "San Francisco",
        "salary": "$120k - $180k",
        "technologies": ["Python", "React", "PostgreSQL"],
        "funding_stage": "Series B",
        "indexed_at": "2025-01-20T12:00:00"
    }
    
    # Try to index directly (this will auto-create the index)
    doc_id = "sample_company"
    response = es._request("PUT", f"/{es.index}/_doc/{doc_id}", sample_job)
    
    if response and response.status_code in [200, 201]:
        print("✅ Direct indexing successful!")
        print("   Index was auto-created with the document")
        
        # Verify the document exists
        retrieved = es.get_document(doc_id)
        if retrieved and retrieved.get('found'):
            print("✅ Document retrieval successful!")
            print(f"   Company: {retrieved['_source'].get('company_name')}")
        
        # Clean up
        es._request("DELETE", f"/{es.index}/_doc/{doc_id}")
        print("✅ Test document cleaned up")
        
        return True
    else:
        print(f"❌ Direct indexing failed: {response.status_code if response else 'No response'}")
        if response:
            print(f"   Error: {response.text}")
        return False

if __name__ == "__main__":
    test_index_creation()
    try_simplified_indexing() 