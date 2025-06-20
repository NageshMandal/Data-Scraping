#!/usr/bin/env python3
"""
Test Elasticsearch Connection
============================

Test the Elasticsearch connection with API key authentication
to ensure indexing will work properly.
"""

import os
import time
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import urllib3

# Load environment variables
load_dotenv()

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "project_jobposters_index")
ES_API_KEY = os.getenv("ES_API_KEY")
ES_USER = os.getenv("ES_USER", "project_jobposters_user")
ES_PASS = os.getenv("ES_PASS", "project_jobposters_1234")

def test_elasticsearch():
    """Test Elasticsearch connection and basic operations"""
    
    print("🔍 Testing Elasticsearch Connection")
    print("=" * 50)
    
    # Check environment variables
    print(f"📍 ES_HOST: {ES_HOST}")
    print(f"📂 ES_INDEX: {ES_INDEX}")
    
    if ES_API_KEY:
        print("🔑 API Key found - using API key authentication")
        es = Elasticsearch(
            ES_HOST,
            api_key=ES_API_KEY,
            verify_certs=False,
            ssl_show_warn=False,
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=3
        )
    else:
        print("🔐 No API key found - using basic authentication")
        print(f"👤 ES_USER: {ES_USER}")
        es = Elasticsearch(
            ES_HOST,
            basic_auth=(ES_USER, ES_PASS),
            verify_certs=False,
            ssl_show_warn=False,
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=3
        )
    
    try:
        # Test 1: Basic connection
        print("\n🔗 Testing basic connection...")
        info = es.info()
        print(f"✅ Connected to Elasticsearch {info['version']['number']}")
        print(f"📊 Cluster: {info['cluster_name']}")
        
        # Test 2: Check if index exists
        print(f"\n📂 Checking index '{ES_INDEX}'...")
        if es.indices.exists(index=ES_INDEX):
            print(f"✅ Index '{ES_INDEX}' exists")
            
            # Get index stats
            stats = es.indices.stats(index=ES_INDEX)
            doc_count = stats['indices'][ES_INDEX]['total']['docs']['count']
            print(f"📊 Documents in index: {doc_count}")
        else:
            print(f"⚠️ Index '{ES_INDEX}' does not exist")
            print("💡 Will be created automatically when first document is indexed")
        
        # Test 3: Test document operations
        print(f"\n🧪 Testing document operations...")
        test_doc = {
            "test": True,
            "timestamp": time.time(),
            "message": "Elasticsearch connection test"
        }
        
        # Index test document
        result = es.index(index=f"{ES_INDEX}_test", id="connection_test", body=test_doc)
        print(f"✅ Test document indexed: {result['result']}")
        
        # Retrieve test document
        doc = es.get(index=f"{ES_INDEX}_test", id="connection_test")
        print(f"✅ Test document retrieved: {doc['found']}")
        
        # Delete test document
        es.delete(index=f"{ES_INDEX}_test", id="connection_test")
        print("✅ Test document deleted")
        
        # Delete test index
        if es.indices.exists(index=f"{ES_INDEX}_test"):
            es.indices.delete(index=f"{ES_INDEX}_test")
            print("✅ Test index cleaned up")
        
        print(f"\n🎉 All tests passed! Elasticsearch is ready for job data indexing.")
        return True
        
    except Exception as e:
        print(f"\n❌ Elasticsearch connection failed: {e}")
        
        # Provide troubleshooting suggestions
        print("\n🔧 Troubleshooting suggestions:")
        print("1. Check if ES_API_KEY is set correctly in .env file")
        print("2. Verify ES_HOST URL is correct")
        print("3. Ensure Elasticsearch cluster is running")
        print("4. Check if API key has proper permissions")
        print("5. Try basic auth as fallback if API key fails")
        
        return False

if __name__ == "__main__":
    success = test_elasticsearch()
    
    if success:
        print("\n✅ Ready to run classification with Elasticsearch indexing!")
    else:
        print("\n❌ Fix Elasticsearch connection before running classification.") 