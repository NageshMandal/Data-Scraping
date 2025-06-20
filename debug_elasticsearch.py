#!/usr/bin/env python3
"""
Debug Elasticsearch Connection Issues
===================================

Test different connection methods to identify the exact issue.
"""

import os
import requests
import urllib3
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
ES_HOST = os.getenv("ES_HOST")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS")

# Security validation
if not ES_HOST:
    print("❌ ES_HOST not found in environment variables!")
    print("💡 Please set ES_HOST in your .env file")
    exit(1)

if not ES_PASS:
    print("❌ ES_PASS not found in environment variables!")
    print("💡 Please set ES_PASS in your .env file")
    exit(1)

def test_with_requests():
    """Test with basic requests library (like curl)"""
    print("🔧 Testing with Python requests (like curl)...")
    
    try:
        url = f"{ES_HOST}/_cluster/health"
        response = requests.get(
            url,
            auth=(ES_USER, ES_PASS),
            verify=False,  # Ignore SSL certificates
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ Requests library works!")
            print(f"   Status: {response.json()['status']}")
            print(f"   Cluster: {response.json()['cluster_name']}")
            return True
        else:
            print(f"❌ Requests failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Requests error: {e}")
        return False

def test_elasticsearch_client_v1():
    """Test with basic Elasticsearch client"""
    print("\n🔧 Testing Elasticsearch client (basic config)...")
    
    try:
        es = Elasticsearch(
            ES_HOST,
            basic_auth=(ES_USER, ES_PASS),
            verify_certs=False,
            ssl_show_warn=False
        )
        
        # Test ping
        if es.ping():
            print("✅ Basic Elasticsearch client works!")
            info = es.info()
            print(f"   Version: {info['version']['number']}")
            return True
        else:
            print("❌ Elasticsearch ping failed (basic config)")
            return False
            
    except Exception as e:
        print(f"❌ Elasticsearch client error (basic): {e}")
        return False

def test_elasticsearch_client_v2():
    """Test with enhanced Elasticsearch client"""
    print("\n🔧 Testing Elasticsearch client (enhanced config)...")
    
    try:
        es = Elasticsearch(
            ES_HOST,
            basic_auth=(ES_USER, ES_PASS),
            verify_certs=False,
            ssl_show_warn=False,
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=3,
            ca_certs=False,
            ssl_assert_hostname=False,
            ssl_assert_fingerprint=False
        )
        
        # Test ping
        if es.ping():
            print("✅ Enhanced Elasticsearch client works!")
            info = es.info()
            print(f"   Version: {info['version']['number']}")
            return True
        else:
            print("❌ Elasticsearch ping failed (enhanced config)")
            return False
            
    except Exception as e:
        print(f"❌ Elasticsearch client error (enhanced): {e}")
        return False

def test_elasticsearch_client_v3():
    """Test with minimal Elasticsearch client"""
    print("\n🔧 Testing Elasticsearch client (minimal config)...")
    
    try:
        # Try with just the host and auth
        es = Elasticsearch(
            [ES_HOST],
            http_auth=(ES_USER, ES_PASS),
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False,
            timeout=20
        )
        
        # Test ping
        if es.ping():
            print("✅ Minimal Elasticsearch client works!")
            info = es.info()
            print(f"   Version: {info['version']['number']}")
            return True
        else:
            print("❌ Elasticsearch ping failed (minimal config)")
            return False
            
    except Exception as e:
        print(f"❌ Elasticsearch client error (minimal): {e}")
        return False

def main():
    print("🔍 Elasticsearch Connection Debug")
    print("=" * 50)
    print(f"Host: {ES_HOST}")
    print(f"User: {ES_USER}")
    print(f"Pass: {ES_PASS[:6]}...")
    print()
    
    # Test different methods
    methods = [
        test_with_requests,
        test_elasticsearch_client_v1,
        test_elasticsearch_client_v2,
        test_elasticsearch_client_v3
    ]
    
    results = []
    for test_func in methods:
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("📋 Debug Summary:")
    test_names = ["Requests", "ES Basic", "ES Enhanced", "ES Minimal"]
    for i, (name, result) in enumerate(zip(test_names, results)):
        status = "✅ Working" if result else "❌ Failed"
        print(f"   {name}: {status}")
    
    if any(results):
        working_methods = [name for name, result in zip(test_names, results) if result]
        print(f"\n💡 Working methods: {', '.join(working_methods)}")
        print("Use the working configuration in your main code!")
    else:
        print("\n❌ All methods failed - check network/credentials")

if __name__ == "__main__":
    main() 