#!/usr/bin/env python3
"""
Elasticsearch Interface Using Requests
=====================================

Simple Elasticsearch interface using requests library since it works
perfectly with the server (bypassing the problematic ES client).
"""

import os
import json
import requests
import urllib3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ElasticsearchRequests:
    """Simple Elasticsearch interface using requests library"""
    
    def __init__(self):
        self.host = os.getenv("ES_HOST", "https://65.108.41.233:9200")
        self.user = os.getenv("ES_USER", "elastic")
        self.password = os.getenv("ES_PASS", "gXpID1MQcRxP")
        self.index = os.getenv("ES_INDEX", "project_jobposters_index")
        
        # Remove trailing slash if present
        if self.host.endswith('/'):
            self.host = self.host[:-1]
    
    def _request(self, method, endpoint, data=None):
        """Make authenticated request to Elasticsearch"""
        url = f"{self.host}{endpoint}"
        
        headers = {"Content-Type": "application/json"}
        auth = (self.user, self.password)
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, auth=auth, verify=False, timeout=30, headers=headers)
            elif method.upper() == "POST":
                response = requests.post(url, auth=auth, verify=False, timeout=30, 
                                       headers=headers, data=json.dumps(data) if data else None)
            elif method.upper() == "PUT":
                response = requests.put(url, auth=auth, verify=False, timeout=30, 
                                      headers=headers, data=json.dumps(data) if data else None)
            elif method.upper() == "DELETE":
                response = requests.delete(url, auth=auth, verify=False, timeout=30, headers=headers)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            return response
        except Exception as e:
            print(f"❌ Request failed: {e}")
            return None
    
    def ping(self):
        """Test connection to Elasticsearch"""
        response = self._request("GET", "/")
        return response and response.status_code == 200
    
    def cluster_health(self):
        """Get cluster health"""
        response = self._request("GET", "/_cluster/health")
        if response and response.status_code == 200:
            return response.json()
        return None
    
    def index_exists(self, index_name=None):
        """Check if index exists"""
        index_name = index_name or self.index
        response = self._request("GET", f"/{index_name}")
        return response and response.status_code == 200
    
    def create_index(self, index_name=None, mapping=None):
        """Create index with optional mapping"""
        index_name = index_name or self.index
        
        # Default mapping for job data
        if mapping is None:
            mapping = {
                "mappings": {
                    "properties": {
                        "company": {
                            "properties": {
                                "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                                "domain": {"type": "keyword"},
                                "industries": {"type": "keyword"},
                                "size": {"type": "keyword"},
                                "funding": {"type": "text"},
                                "founder": {"type": "text"}
                            }
                        },
                        "classification": {
                            "properties": {
                                "categories": {"type": "keyword"},
                                "company_stage": {"type": "keyword"},
                                "hiring_urgency": {"type": "keyword"}
                            }
                        },
                        "prospecting": {
                            "properties": {
                                "investment_readiness": {"type": "keyword"},
                                "key_technologies": {"type": "keyword"},
                                "contact_potential": {"type": "keyword"}
                            }
                        },
                        "jobs": {
                            "type": "nested",
                            "properties": {
                                "position": {"type": "text"},
                                "location": {"type": "keyword"},
                                "salary": {"type": "text"},
                                "department": {"type": "keyword"},
                                "seniority": {"type": "keyword"}
                            }
                        },
                        "keywords": {"type": "keyword"},
                        "summary": {"type": "text"},
                        "created_at": {"type": "date"},
                        "last_updated": {"type": "date"}
                    }
                }
            }
        
        response = self._request("PUT", f"/{index_name}", mapping)
        return response and response.status_code == 200
    
    def index_document(self, doc_id, document, index_name=None):
        """Index a single document"""
        index_name = index_name or self.index
        response = self._request("PUT", f"/{index_name}/_doc/{doc_id}", document)
        return response and response.status_code in [200, 201]
    
    def get_document(self, doc_id, index_name=None):
        """Get a document by ID"""
        index_name = index_name or self.index
        response = self._request("GET", f"/{index_name}/_doc/{doc_id}")
        if response and response.status_code == 200:
            return response.json()
        return None
    
    def search(self, query, index_name=None):
        """Search documents"""
        index_name = index_name or self.index
        response = self._request("POST", f"/{index_name}/_search", query)
        if response and response.status_code == 200:
            return response.json()
        return None
    
    def index_stats(self, index_name=None):
        """Get index statistics"""
        index_name = index_name or self.index
        response = self._request("GET", f"/{index_name}/_stats")
        if response and response.status_code == 200:
            stats = response.json()
            return {
                "doc_count": stats["indices"][index_name]["total"]["docs"]["count"],
                "size": stats["indices"][index_name]["total"]["store"]["size_in_bytes"]
            }
        return None

def test_elasticsearch_requests():
    """Test the requests-based Elasticsearch interface"""
    print("🧪 Testing Elasticsearch Requests Interface")
    print("=" * 50)
    
    es = ElasticsearchRequests()
    
    # Test connection
    print("🔗 Testing connection...")
    if es.ping():
        print("✅ Connection successful!")
        
        # Get cluster health
        health = es.cluster_health()
        if health:
            print(f"   Cluster: {health['cluster_name']}")
            print(f"   Status: {health['status']}")
            print(f"   Nodes: {health['number_of_nodes']}")
    else:
        print("❌ Connection failed!")
        return False
    
    # Check if index exists
    print(f"\n📂 Checking index '{es.index}'...")
    if es.index_exists():
        print("✅ Index exists!")
        stats = es.index_stats()
        if stats:
            print(f"   Documents: {stats['doc_count']}")
            print(f"   Size: {stats['size']:,} bytes")
    else:
        print("⚠️ Index does not exist")
        print("💡 Will be created when first document is indexed")
    
    # Test document operations
    print(f"\n🧪 Testing document operations...")
    test_doc = {
        "test": True,
        "message": "Elasticsearch requests test",
        "timestamp": "2025-01-20T12:00:00"
    }
    
    test_index = f"{es.index}_test"
    
    # Index test document
    if es.index_document("test_doc", test_doc, test_index):
        print("✅ Document indexed successfully")
        
        # Retrieve test document
        retrieved = es.get_document("test_doc", test_index)
        if retrieved and retrieved.get("found"):
            print("✅ Document retrieved successfully")
        else:
            print("❌ Document retrieval failed")
        
        # Clean up test index
        cleanup_response = es._request("DELETE", f"/{test_index}")
        if cleanup_response and cleanup_response.status_code == 200:
            print("✅ Test index cleaned up")
    else:
        print("❌ Document indexing failed")
    
    print(f"\n🎉 Elasticsearch requests interface is working!")
    return True

if __name__ == "__main__":
    test_elasticsearch_requests() 