"""
ZenRows API Client Utility
==========================

Shared utility module for ZenRows web scraping API integration.
Handles anti-bot measures, captcha solving, and proxy rotation automatically.

Usage:
    from zenrows_client import ZenRowsClient
    
    client = ZenRowsClient()
    html = client.request(url)
"""

import os
import time
import random
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ZenRowsClient:
    """ZenRows API client with built-in error handling and retry logic"""
    
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("ZENROWS_API_KEY")
        if not self.api_key:
            raise RuntimeError("ZENROWS_API_KEY not found in environment variables")
        
        self.api_url = "https://api.zenrows.com/v1/"
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
    
    def request(self, url, **kwargs):
        """
        Make a ZenRows API request with customizable parameters
        
        Args:
            url (str): Target URL to scrape
            **kwargs: Additional ZenRows parameters
                - js_render (bool): Enable JavaScript rendering (default: True)
                - premium_proxy (bool): Use premium proxies (default: True)  
                - block_resources (bool): Block images/CSS/fonts (default: True)
                - wait (int): Wait time in milliseconds (default: 3000)
                - wait_for (str): CSS selector to wait for
                - custom_headers (dict): Custom HTTP headers
                - session_id (str): Session ID for maintaining state
        
        Returns:
            str: HTML content or None if failed
        """
        # Default parameters optimized for job scraping
        params = {
            'apikey': self.api_key,
            'url': url,
            'js_render': str(kwargs.get('js_render', True)).lower(),
            'premium_proxy': str(kwargs.get('premium_proxy', True)).lower(),
            'wait': str(kwargs.get('wait', 3000)),
        }
        
        # Add block_resources if specified (comma-separated resource types)
        block_resources = kwargs.get('block_resources', 'image,stylesheet,font,media')
        if block_resources:
            params['block_resources'] = block_resources
        
        # Optional parameters
        if 'wait_for' in kwargs:
            params['wait_for'] = kwargs['wait_for']
        if 'custom_headers' in kwargs:
            params['custom_headers'] = kwargs['custom_headers']
        if 'session_id' in kwargs:
            params['session_id'] = kwargs['session_id']
        
        self.request_count += 1
        print(f"🔄 ZenRows request #{self.request_count}: {url}")
        
        try:
            response = requests.get(
                self.api_url, 
                params=params, 
                timeout=kwargs.get('timeout', 60)
            )
            
            if response.status_code == 200:
                self.success_count += 1
                print(f"✅ Success - Response length: {len(response.text)}")
                return response.text
                
            elif response.status_code == 422:
                self.error_count += 1
                print("⚠️ ZenRows: Unprocessable request - URL might be blocked or invalid")
                return None
                
            elif response.status_code == 429:
                self.error_count += 1
                print("⚠️ ZenRows: Rate limit hit - waiting...")
                time.sleep(kwargs.get('rate_limit_wait', 10))
                return None
                
            elif response.status_code == 403:
                self.error_count += 1
                print("❌ ZenRows: Forbidden - Check your API key and credits")
                return None
                
            else:
                self.error_count += 1
                print(f"❌ ZenRows error: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            self.error_count += 1
            print("⏰ ZenRows request timeout")
            return None
            
        except Exception as e:
            self.error_count += 1
            print(f"❌ ZenRows request failed: {e}")
            return None
    
    def request_with_retry(self, url, max_retries=3, **kwargs):
        """
        Make a ZenRows request with automatic retry logic
        
        Args:
            url (str): Target URL to scrape
            max_retries (int): Maximum number of retry attempts
            **kwargs: ZenRows parameters (same as request method)
        
        Returns:
            str: HTML content or None if all retries failed
        """
        for attempt in range(1, max_retries + 1):
            print(f"  Attempt {attempt}/{max_retries}")
            
            html_content = self.request(url, **kwargs)
            
            if html_content:
                return html_content
            
            if attempt < max_retries:
                # Exponential backoff with jitter
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"  😴 Waiting {wait_time:.1f}s before retry...")
                time.sleep(wait_time)
        
        print(f"  ❌ Failed after {max_retries} attempts")
        return None
    
    def get_stats(self):
        """Get client usage statistics"""
        return {
            "total_requests": self.request_count,
            "successful_requests": self.success_count,
            "failed_requests": self.error_count,
            "success_rate": (self.success_count / self.request_count * 100) if self.request_count > 0 else 0
        }
    
    def print_stats(self):
        """Print formatted statistics"""
        stats = self.get_stats()
        print(f"\n📊 ZenRows Client Statistics:")
        print(f"   Total Requests: {stats['total_requests']}")
        print(f"   Successful: {stats['successful_requests']}")
        print(f"   Failed: {stats['failed_requests']}")
        print(f"   Success Rate: {stats['success_rate']:.1f}%")

# Convenience functions for quick usage
def zenrows_request(url, **kwargs):
    """Quick single request function"""
    client = ZenRowsClient()
    return client.request(url, **kwargs)

def zenrows_request_with_retry(url, max_retries=3, **kwargs):
    """Quick request with retry function"""
    client = ZenRowsClient()
    return client.request_with_retry(url, max_retries, **kwargs)

# Preset configurations for different scraping scenarios
PRESETS = {
    'job_listing': {
        'js_render': True,
        'premium_proxy': True,
        'block_resources': 'image,stylesheet,font,media',  # Optimized for speed
        'wait': 3000,
        'wait_for': '.text-brand-burgandy'
    },
    'job_detail': {
        'js_render': True,
        'premium_proxy': True,
        'block_resources': 'image,stylesheet,font,media',  # Keep scripts for functionality
        'wait': 5000,
        'wait_for': '[data-test="JobListing"]'
    },
    'fast_scrape': {
        'js_render': False,
        'premium_proxy': False,
        'block_resources': 'image,stylesheet,script,font,media',  # Block everything for speed
        'wait': 1000
    }
}

def get_preset(preset_name):
    """Get predefined configuration preset"""
    return PRESETS.get(preset_name, {}) 