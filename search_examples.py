#!/usr/bin/env python3
"""
Elasticsearch Search Examples
=============================

Demonstrate powerful search capabilities for your 509 indexed jobs.
Perfect for prospecting, market research, and business intelligence.
"""

from elasticsearch_requests import ElasticsearchRequests
import json

def search_examples():
    """Show practical search examples for prospecting"""
    
    print("🔍 Elasticsearch Search Examples for Prospecting")
    print("=" * 55)
    
    es = ElasticsearchRequests()
    
    if not es.ping():
        print("❌ Elasticsearch connection failed!")
        return
    
    print("✅ Connected to Elasticsearch")
    print(f"📂 Searching index: {es.index}")
    
    # Example 1: Find AI/ML companies
    print(f"\n🤖 Example 1: AI/ML Companies")
    ai_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"categories": "AI"}},
                    {"match": {"categories": "Machine Learning"}},
                    {"match": {"key_technologies": "AI"}},
                    {"match": {"company_industries": "Artificial Intelligence"}}
                ]
            }
        },
        "size": 5
    }
    
    results = es.search(ai_query)
    if results:
        # Handle different ES versions (total can be int or dict)
        total = results['hits']['total']
        if isinstance(total, dict):
            total_count = total['value']
        else:
            total_count = total
            
        print(f"   Found {total_count} AI/ML companies")
        for hit in results['hits']['hits'][:3]:
            source = hit['_source']
            print(f"   🏢 {source.get('company_name')} - {source.get('job_position')}")
            print(f"      Categories: {source.get('categories', [])}")
            print(f"      Technologies: {source.get('key_technologies', [])}")
    
    # Example 2: High-growth companies (Series B+)
    print(f"\n📈 Example 2: High-Growth Companies (Series B+)")
    growth_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"company_stage": "Series B"}},
                    {"match": {"company_stage": "Series C"}},
                    {"match": {"company_stage": "Series D"}},
                    {"match": {"investment_readiness": "High"}}
                ]
            }
        },
        "size": 5
    }
    
    results = es.search(growth_query)
    if results:
        total = results['hits']['total']
        total_count = total['value'] if isinstance(total, dict) else total
        print(f"   Found {total_count} high-growth companies")
        for hit in results['hits']['hits'][:3]:
            source = hit['_source']
            print(f"   💰 {source.get('company_name')} - {source.get('company_funding')}")
            print(f"      Stage: {source.get('company_stage')}")
            print(f"      Investment Readiness: {source.get('investment_readiness')}")
    
    # Example 3: Remote-friendly companies
    print(f"\n🏠 Example 3: Remote-Friendly Companies")
    remote_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"job_location": "Remote"}},
                    {"match": {"job_remote_policy": "Remote"}},
                    {"match": {"job_remote_policy": "Hybrid"}}
                ]
            }
        },
        "size": 5
    }
    
    results = es.search(remote_query)
    if results:
        total = results['hits']['total']
        total_count = total['value'] if isinstance(total, dict) else total
        print(f"   Found {total_count} remote-friendly opportunities")
        for hit in results['hits']['hits'][:3]:
            source = hit['_source']
            print(f"   🌍 {source.get('company_name')} - {source.get('job_position')}")
            print(f"      Location: {source.get('job_location')}")
            print(f"      Remote Policy: {source.get('job_remote_policy')}")
    
    # Example 4: Companies with specific technologies
    print(f"\n⚙️ Example 4: Python/React Companies")
    tech_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"key_technologies": "Python"}},
                    {"match": {"key_technologies": "React"}},
                    {"match": {"job_skills": "Python"}},
                    {"match": {"job_skills": "React"}}
                ]
            }
        },
        "size": 5
    }
    
    results = es.search(tech_query)
    if results:
        total = results['hits']['total']
        total_count = total['value'] if isinstance(total, dict) else total
        print(f"   Found {total_count} Python/React companies")
        for hit in results['hits']['hits'][:3]:
            source = hit['_source']
            print(f"   💻 {source.get('company_name')} - {source.get('job_position')}")
            print(f"      Technologies: {source.get('key_technologies', [])}")
            print(f"      Skills: {source.get('job_skills', [])}")
    
    # Example 5: High contact potential companies
    print(f"\n📞 Example 5: High Contact Potential Companies")
    contact_query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"contact_potential": "High"}},
                    {"match": {"hiring_urgency": "High"}}
                ]
            }
        },
        "size": 5
    }
    
    results = es.search(contact_query)
    if results:
        total = results['hits']['total']
        total_count = total['value'] if isinstance(total, dict) else total
        print(f"   Found {total_count} high-potential prospects")
        for hit in results['hits']['hits'][:3]:
            source = hit['_source']
            print(f"   🎯 {source.get('company_name')} - {source.get('job_position')}")
            print(f"      Contact Potential: {source.get('contact_potential')}")
            print(f"      Hiring Urgency: {source.get('hiring_urgency')}")
            print(f"      Domain: {source.get('company_domain')}")

def aggregation_examples():
    """Show aggregation examples for business intelligence"""
    
    print(f"\n📊 Business Intelligence Examples")
    print("=" * 35)
    
    es = ElasticsearchRequests()
    
    # Top technologies
    print(f"\n🔧 Top Technologies in Dataset:")
    tech_agg = {
        "size": 0,
        "aggs": {
            "top_technologies": {
                "terms": {
                    "field": "key_technologies.keyword",
                    "size": 10
                }
            }
        }
    }
    
    results = es.search(tech_agg)
    if results and 'aggregations' in results:
        for bucket in results['aggregations']['top_technologies']['buckets']:
            print(f"   {bucket['key']}: {bucket['doc_count']} companies")
    
    # Top company stages
    print(f"\n💰 Company Stages Distribution:")
    stage_agg = {
        "size": 0,
        "aggs": {
            "company_stages": {
                "terms": {
                    "field": "company_stage.keyword",
                    "size": 10
                }
            }
        }
    }
    
    results = es.search(stage_agg)
    if results and 'aggregations' in results:
        for bucket in results['aggregations']['company_stages']['buckets']:
            print(f"   {bucket['key']}: {bucket['doc_count']} companies")
    
    # Top job locations
    print(f"\n🌍 Top Job Locations:")
    location_agg = {
        "size": 0,
        "aggs": {
            "top_locations": {
                "terms": {
                    "field": "job_location.keyword",
                    "size": 10
                }
            }
        }
    }
    
    results = es.search(location_agg)
    if results and 'aggregations' in results:
        for bucket in results['aggregations']['top_locations']['buckets']:
            print(f"   {bucket['key']}: {bucket['doc_count']} jobs")

if __name__ == "__main__":
    search_examples()
    aggregation_examples()
    
    print(f"\n🎉 Your 509 classified jobs are now a powerful prospecting database!")
    print(f"🔍 Use these search patterns to find:")
    print(f"   • Investment opportunities by stage/technology")
    print(f"   • Sales prospects by contact potential")
    print(f"   • Market trends by location/industry")
    print(f"   • Hiring patterns by company size/urgency") 