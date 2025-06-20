"""
Database Connectivity Test Script
=================================
Tests MongoDB and Elasticsearch connections and shows current data
"""

import json
import os
from datetime import datetime

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from pymongo import MongoClient

# Load environment variables
load_dotenv()


def test_mongodb():
    """Test MongoDB connection and show data"""
    print("🔍 Testing MongoDB Connection...")

    try:
        # Get MongoDB configuration
        MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        DB_NAME = os.getenv("MONGO_DB_NAME", "job_scraping")

        print(f"MongoDB URI: {MONGO_URI}")
        print(f"Database: {DB_NAME}")

        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]

        # Test connection
        client.admin.command("ping")
        print("✅ MongoDB connection successful!")

        # Show collections
        collections = db.list_collection_names()
        print(f"\n📂 Collections found: {collections}")

        # Check each collection
        for collection_name in ["job_urls", "jobs", "classified_jobs"]:
            collection = db[collection_name]
            count = collection.count_documents({})
            print(f"\n📊 Collection: {collection_name}")
            print(f"   Total documents: {count}")

            if count > 0:
                # Show sample document
                sample = collection.find_one()
                print(
                    f"   Sample document keys: {list(sample.keys()) if sample else 'None'}"
                )

                # Show specific stats for each collection
                if collection_name == "job_urls":
                    scraped_count = collection.count_documents({"scraped": True})
                    pending_count = collection.count_documents({"scraped": False})
                    print(f"   Scraped URLs: {scraped_count}")
                    print(f"   Pending URLs: {pending_count}")

                    # Show some sample URLs
                    print(f"   Sample URLs:")
                    for url_doc in collection.find().limit(3):
                        status = "✅" if url_doc.get("scraped", False) else "⏳"
                        print(f"     {status} {url_doc.get('url', 'No URL')}")

                elif collection_name == "jobs":
                    # Show job statistics
                    pipeline = [
                        {"$group": {"_id": "$company_name", "count": {"$sum": 1}}},
                        {"$sort": {"count": -1}},
                        {"$limit": 5},
                    ]
                    top_companies = list(collection.aggregate(pipeline))
                    if top_companies:
                        print(f"   Top companies by job count:")
                        for company in top_companies:
                            print(f"     {company['_id']}: {company['count']} jobs")

                elif collection_name == "classified_jobs":
                    # Show classification statistics
                    sample_classified = collection.find_one()
                    if sample_classified:
                        print(
                            f"   Sample classified job keys: {list(sample_classified.keys())}"
                        )

        client.close()
        return True

    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return False


def test_elasticsearch():
    """Test Elasticsearch connection and show data"""
    print("\n🔍 Testing Elasticsearch Connection...")

    try:
        # Get Elasticsearch configuration with new credentials
        ES_HOST = os.getenv("ES_HOST", "https://65.108.41.233:9200")
        ES_INDEX = os.getenv("ES_INDEX", "project_jobposters_index")
        ES_API_KEY = os.getenv("ES_API_KEY")  # API key takes priority
        ES_USER = os.getenv("ES_USER", "elastic")  # New default username
        ES_PASS = os.getenv("ES_PASS", "gXpID1MQcRxP")  # New default password

        print(f"Elasticsearch Host: {ES_HOST}")
        print(f"Index: {ES_INDEX}")
        
        # Import SSL handling
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Configure Elasticsearch client with basic auth as primary (since curl works)
        print(f"🔐 Using Basic authentication - User: {ES_USER}")
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

        # Test connection
        if es.ping():
            print("✅ Elasticsearch connection successful!")
        else:
            print("❌ Elasticsearch ping failed")
            return False

        # Show cluster info
        info = es.info()
        print(f"   Cluster: {info['cluster_name']}")
        print(f"   Version: {info['version']['number']}")

        # Check if index exists
        if es.indices.exists(index=ES_INDEX):
            print(f"\n📂 Index '{ES_INDEX}' exists!")

            # Get index stats
            stats = es.indices.stats(index=ES_INDEX)
            doc_count = stats["indices"][ES_INDEX]["total"]["docs"]["count"]
            print(f"   Total documents: {doc_count}")

            if doc_count > 0:
                # Show sample documents
                search_result = es.search(
                    index=ES_INDEX, body={"query": {"match_all": {}}, "size": 3}
                )

                print(f"   Sample documents:")
                for hit in search_result["hits"]["hits"]:
                    source = hit["_source"]
                    company_name = source.get("company", {}).get("name", "Unknown")
                    job_count = len(source.get("jobs", []))
                    categories = source.get("categories", [])
                    print(f"     🏢 {company_name} - {job_count} jobs - {categories}")

                # Show aggregations
                try:
                    agg_result = es.search(
                        index=ES_INDEX,
                        body={
                            "size": 0,
                            "aggs": {
                                "top_categories": {
                                    "terms": {"field": "categories.keyword", "size": 5}
                                },
                                "job_count_stats": {
                                    "stats": {"script": "doc['jobs'].size()"}
                                },
                            },
                        },
                    )

                    print(f"\n📊 Top Categories:")
                    for bucket in agg_result["aggregations"]["top_categories"]["buckets"]:
                        print(f"     {bucket['key']}: {bucket['doc_count']} companies")

                    job_stats = agg_result["aggregations"]["job_count_stats"]
                    print(f"\n📈 Job Count Statistics:")
                    print(f"     Average jobs per company: {job_stats['avg']:.1f}")
                    print(f"     Max jobs from one company: {int(job_stats['max'])}")
                    print(f"     Total jobs indexed: {int(job_stats['sum'])}")
                except Exception as agg_error:
                    print(f"   ⚠️ Aggregation error (index might be empty): {agg_error}")
        else:
            print(f"\n📂 Index '{ES_INDEX}' does not exist yet")
            print("   This is normal if you haven't run the classification step")

        return True

    except Exception as e:
        print(f"❌ Elasticsearch error: {e}")
        print("\n🔧 Troubleshooting suggestions:")
        print("1. Check if ES_HOST is correct (should be port 9200)")
        print("2. Verify ES_USER and ES_PASS are correct")
        print("3. Check if ES_API_KEY is set and valid")
        print("4. Ensure Elasticsearch cluster is running and accessible")
        print("5. Check network connectivity to the host")
        return False


def show_environment_status():
    """Show environment configuration"""
    print("🔧 Environment Configuration:")

    env_vars = [
        "MONGO_URI",
        "MONGO_DB_NAME",
        "ZENROWS_API_KEY",
        "GROQ_API_KEY",
        "ES_HOST",
        "ES_INDEX",
        "ES_API_KEY",  # Added API key support
        "ES_USER",
        "ES_PASS",
    ]

    for var in env_vars:
        value = os.getenv(var)
        if value:
            if "API_KEY" in var or "PASS" in var:
                # Hide sensitive values
                display_value = value[:10] + "..." if len(value) > 10 else "***"
            elif "URI" in var and "mongodb" in value:
                # Hide MongoDB password
                display_value = (
                    value.split("://")[0] + "://***@" + value.split("@")[-1]
                    if "@" in value
                    else value
                )
            else:
                display_value = value
            print(f"   ✅ {var}: {display_value}")
        else:
            print(f"   ❌ {var}: Not set")


def main():
    print("🚀 Database Connectivity Test")
    print("=" * 50)

    # Show environment status
    show_environment_status()
    print()

    # Test MongoDB
    mongo_success = test_mongodb()

    # Test Elasticsearch
    es_success = test_elasticsearch()

    # Summary
    print("\n" + "=" * 50)
    print("📋 Test Summary:")
    print(f"   MongoDB: {'✅ Working' if mongo_success else '❌ Failed'}")
    print(f"   Elasticsearch: {'✅ Working' if es_success else '❌ Failed'}")

    if mongo_success and es_success:
        print("\n🎉 All database connections are working!")
    elif mongo_success:
        print("\n⚠️ MongoDB working, Elasticsearch needs setup")
    elif es_success:
        print("\n⚠️ Elasticsearch working, MongoDB needs setup")
    else:
        print("\n❌ Both databases need configuration")


if __name__ == "__main__":
    main()

