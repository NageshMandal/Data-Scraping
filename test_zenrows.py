"""
ZenRows API Test Script
======================
Quick test to diagnose ZenRows integration issues
"""

import os
from dotenv import load_dotenv
from zenrows_client import ZenRowsClient

def test_zenrows():
    print("🔍 Testing ZenRows setup...")
    
    # Load environment variables
    load_dotenv()
    
    # Check API key
    api_key = os.getenv("ZENROWS_API_KEY")
    print(f"API Key present: {bool(api_key)}")
    if api_key:
        print(f"API Key length: {len(api_key)}")
        print(f"API Key starts with: {api_key[:10]}...")
    
    try:
        # Test client creation
        client = ZenRowsClient()
        print("✅ ZenRows client created successfully")
        
        # Test 1: Simple request without blocking
        print("\n🔄 Test 1: Simple HTTP request...")
        result = client.request(
            'https://httpbin.org/json',
            js_render=False,
            premium_proxy=False,
            block_resources=None
        )
        
        if result:
            print("✅ Basic ZenRows API working!")
            print(f"Response length: {len(result)}")
        else:
            print("❌ Basic ZenRows API call failed")
            return False
        
        # Test 2: Request without resource blocking (safe)
        print("\n🔄 Test 2: Request without resource blocking...")
        result = client.request(
            'https://httpbin.org/json',
            js_render=False,
            premium_proxy=True,
            block_resources=None
        )
        
        if result:
            print("✅ Request without blocking works!")
        else:
            print("❌ Request without blocking failed")
            return False
        
        # Test 3: Try different resource blocking formats
        print("\n🔄 Test 3: Testing resource blocking formats...")
        test_resources = [
            "image",
            "images", 
            "stylesheet",
            "script",
            "font",
            "media"
        ]
        
        for resource in test_resources:
            print(f"  Testing block_resources='{resource}'...")
            result = client.request(
                'https://httpbin.org/json',
                js_render=False,
                premium_proxy=False,
                block_resources=resource
            )
            if result:
                print(f"    ✅ '{resource}' works!")
            else:
                print(f"    ❌ '{resource}' failed")
        
        # Test 4: Wellfound-like request (without blocking for now)
        print("\n🔄 Test 4: Wellfound test request...")
        result = client.request(
            'https://wellfound.com',
            js_render=True,
            premium_proxy=True,
            block_resources=None
        )
        
        if result:
            print("✅ Wellfound request works!")
            print(f"Response contains 'wellfound': {'wellfound' in result.lower()}")
        else:
            print("❌ Wellfound request failed")
        
        # Print stats
        client.print_stats()
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_zenrows()
    if success:
        print("\n🎉 ZenRows setup looks good!")
    else:
        print("\n❌ ZenRows setup has issues") 