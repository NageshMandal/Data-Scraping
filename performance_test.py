#!/usr/bin/env python3
"""
Performance Test Suite
======================

Validate and benchmark the optimized pipeline performance.
Tests with small datasets to ensure everything works correctly.
"""

import os
import json
import time
import subprocess
import sys
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def create_test_dataset(size=100):
    """Create a small test dataset for performance validation"""
    print(f"📋 Creating test dataset with {size} URLs...")
    
    with open("wellfound_urls.json", "r") as f:
        full_data = json.load(f)
    
    # Take first N URLs and reset their processed status
    test_data = full_data[:size]
    for entry in test_data:
        entry["value"] = False
    
    # Save test dataset
    with open("wellfound_urls_test.json", "w") as f:
        json.dump(test_data, f, indent=2)
    
    print(f"✅ Test dataset saved: wellfound_urls_test.json")
    return len(test_data)

def clear_test_data():
    """Clear test data from MongoDB"""
    print("🧹 Clearing previous test data...")
    
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB_NAME", "job_scraping")]
        
        # Clear test collections
        db["job_urls_test"].delete_many({})
        db["job_data_test"].delete_many({})
        db["classified_jobs_test"].delete_many({})
        
        print("✅ Test data cleared")
        client.close()
        
    except Exception as e:
        print(f"⚠️ Error clearing test data: {e}")

def run_performance_test():
    """Run comprehensive performance test"""
    print("🚀 PERFORMANCE TEST SUITE")
    print("=" * 50)
    
    # Create test dataset
    test_size = create_test_dataset(50)  # Small test set
    
    # Clear previous test data
    clear_test_data()
    
    results = {}
    
    print(f"\n🧪 Testing optimized pipeline with {test_size} URLs...")
    
    # Test 1: URL Scraping Performance
    print(f"\n1️⃣ Testing URL Scraping (Optimized)...")
    start_time = time.time()
    
    try:
        # Temporarily replace the URLs file for testing
        os.rename("wellfound_urls.json", "wellfound_urls_backup.json")
        os.rename("wellfound_urls_test.json", "wellfound_urls.json")
        
        # Run optimized URL scraping
        result = subprocess.run([
            sys.executable, "scrap_urls_optimized.py"
        ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
        
        url_scrape_time = time.time() - start_time
        results["url_scraping"] = {
            "time": url_scrape_time,
            "success": result.returncode == 0,
            "output": result.stdout[-500:] if result.stdout else "",
            "error": result.stderr[-500:] if result.stderr else ""
        }
        
        print(f"   ⏱️ Time: {url_scrape_time:.1f} seconds")
        print(f"   📊 Status: {'✅ Success' if result.returncode == 0 else '❌ Failed'}")
        
    except subprocess.TimeoutExpired:
        results["url_scraping"] = {"time": 300, "success": False, "error": "Timeout"}
        print(f"   ⏱️ Time: Timeout (>300s)")
        print(f"   📊 Status: ❌ Timeout")
    except Exception as e:
        results["url_scraping"] = {"time": 0, "success": False, "error": str(e)}
        print(f"   📊 Status: ❌ Error: {e}")
    finally:
        # Restore original URLs file
        if os.path.exists("wellfound_urls_backup.json"):
            os.rename("wellfound_urls.json", "wellfound_urls_test.json")
            os.rename("wellfound_urls_backup.json", "wellfound_urls.json")
    
    # Check MongoDB for discovered URLs
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB_NAME", "job_scraping")]
        
        discovered_urls = db["job_urls"].count_documents({})
        print(f"   🔍 URLs discovered: {discovered_urls}")
        results["url_scraping"]["urls_found"] = discovered_urls
        
        client.close()
        
    except Exception as e:
        print(f"   ⚠️ Error checking URLs: {e}")
    
    # Test 2: Performance Comparison
    print(f"\n2️⃣ Performance Analysis...")
    
    if results["url_scraping"]["success"]:
        urls_per_second = results["url_scraping"].get("urls_found", 0) / url_scrape_time
        print(f"   ⚡ Processing rate: {urls_per_second:.1f} URLs/second")
        
        # Estimate full pipeline performance
        total_urls = 11600
        estimated_time = total_urls / urls_per_second if urls_per_second > 0 else 0
        print(f"   📈 Estimated full pipeline time: {estimated_time/60:.1f} minutes")
        
        # Compare to sequential (estimated)
        sequential_estimate = total_urls * 3  # Assume 3 seconds per URL sequentially
        speedup = sequential_estimate / estimated_time if estimated_time > 0 else 0
        print(f"   🚀 Estimated speedup: {speedup:.1f}x")
        
        results["performance"] = {
            "urls_per_second": urls_per_second,
            "estimated_full_time": estimated_time,
            "estimated_speedup": speedup
        }
    
    # Test 3: Resource Usage
    print(f"\n3️⃣ Resource Usage Analysis...")
    
    try:
        import psutil
        
        # Get current process info
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        cpu_percent = process.cpu_percent()
        
        print(f"   🧠 Memory usage: {memory_mb:.1f} MB")
        print(f"   🖥️ CPU usage: {cpu_percent:.1f}%")
        
        results["resources"] = {
            "memory_mb": memory_mb,
            "cpu_percent": cpu_percent
        }
        
    except ImportError:
        print(f"   ⚠️ psutil not available for resource monitoring")
    except Exception as e:
        print(f"   ⚠️ Error monitoring resources: {e}")
    
    # Test 4: Rate Limiting Validation
    print(f"\n4️⃣ Rate Limiting Validation...")
    
    if results["url_scraping"]["success"] and url_scrape_time > 0:
        # Check if we stayed within rate limits
        requests_made = results["url_scraping"].get("urls_found", 0)
        avg_rate = requests_made / url_scrape_time
        max_allowed_rate = 5  # ZenRows limit
        
        print(f"   📊 Average request rate: {avg_rate:.2f} req/sec")
        print(f"   🚦 Rate limit compliance: {'✅ Good' if avg_rate <= max_allowed_rate else '⚠️ Over limit'}")
        
        results["rate_limiting"] = {
            "avg_rate": avg_rate,
            "compliant": avg_rate <= max_allowed_rate
        }
    
    # Generate Report
    print(f"\n" + "=" * 50)
    print(f"📊 PERFORMANCE TEST RESULTS")
    print(f"=" * 50)
    
    for test_name, test_results in results.items():
        print(f"\n{test_name.upper()}:")
        for key, value in test_results.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2f}")
            elif isinstance(value, bool):
                print(f"  {key}: {'✅' if value else '❌'}")
            else:
                print(f"  {key}: {value}")
    
    # Save results
    with open("performance_test_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Results saved to: performance_test_results.json")
    
    # Cleanup
    try:
        if os.path.exists("wellfound_urls_test.json"):
            os.remove("wellfound_urls_test.json")
    except:
        pass
    
    # Overall assessment
    success_rate = sum(1 for r in results.values() if isinstance(r, dict) and r.get("success", False))
    total_tests = len([r for r in results.values() if isinstance(r, dict) and "success" in r])
    
    print(f"\n🎯 Overall Test Success: {success_rate}/{total_tests}")
    
    if success_rate >= total_tests * 0.8:  # 80% success rate
        print(f"🎉 PERFORMANCE TEST PASSED!")
        print(f"✅ Optimized pipeline is ready for production use")
    else:
        print(f"⚠️ PERFORMANCE TEST NEEDS ATTENTION")
        print(f"🔧 Review failed tests and optimize further")
    
    return results

def quick_benchmark():
    """Run a quick benchmark test"""
    print("⚡ QUICK BENCHMARK")
    print("=" * 30)
    
    # Test rate limiter performance
    print("Testing rate limiter...")
    
    start = time.time()
    
    # Import rate limiter from optimized script
    try:
        from scrap_urls_optimized import RateLimiter
        
        limiter = RateLimiter(5)  # 5 requests per second
        
        # Make 10 test calls
        for i in range(10):
            call_start = time.time()
            limiter.wait_if_needed()
            call_time = time.time() - call_start
            print(f"  Call {i+1}: {call_time:.3f}s delay")
        
        total_time = time.time() - start
        actual_rate = 10 / total_time
        
        print(f"\n📊 Rate Limiter Results:")
        print(f"   Total time: {total_time:.2f}s")
        print(f"   Actual rate: {actual_rate:.2f} req/sec")
        print(f"   Target rate: 5.0 req/sec")
        print(f"   Compliance: {'✅' if actual_rate <= 5.1 else '❌'}")
        
    except ImportError as e:
        print(f"❌ Could not import rate limiter: {e}")
        print(f"🔧 Make sure scrap_urls_optimized.py exists")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Performance Test Suite')
    parser.add_argument('--quick', action='store_true', help='Run quick benchmark only')
    parser.add_argument('--full', action='store_true', help='Run full performance test')
    
    args = parser.parse_args()
    
    if args.quick:
        quick_benchmark()
    elif args.full:
        run_performance_test()
    else:
        print("Please specify --quick or --full")
        print("  --quick: Fast rate limiter test")
        print("  --full: Complete pipeline performance test") 