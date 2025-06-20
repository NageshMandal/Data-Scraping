#!/usr/bin/env python3
"""
AMD Ryzen 9955HX Linux Optimized URL Scraper
Optimized for 16-core/32-thread AMD systems with high memory
"""

import asyncio
import aiohttp
import json
import logging
import os
import time
import psutil
import platform
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Set
import multiprocessing
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('url_scraping_amd.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AMDLinuxURLScraper:
    def __init__(self):
        self.setup_system_optimization()
        self.zenrows_api_key = os.getenv('ZENROWS_API_KEY')
        if not self.zenrows_api_key:
            raise ValueError("ZENROWS_API_KEY environment variable is required")
        
        self.base_url = "https://api.zenrows.com/v1/"
        self.scraped_urls = set()
        self.total_urls_found = 0
        self.session = None
        
        # AMD Ryzen 9955HX optimized settings
        self.max_workers = min(48, multiprocessing.cpu_count() * 3)  # Aggressive for 32 threads
        self.max_concurrent_requests = 64  # High concurrency for powerful CPU
        self.batch_size = 32  # Large batches for high memory system
        self.rate_limit_delay = 0.3  # Faster rate for powerful system
        
        # Linux-specific optimizations
        self.setup_linux_optimizations()
        
        logger.info(f"AMD Ryzen URL Scraper initialized:")
        logger.info(f"  CPU: {self.cpu_info}")
        logger.info(f"  RAM: {self.memory_gb}GB")
        logger.info(f"  Max Workers: {self.max_workers}")
        logger.info(f"  Concurrent Requests: {self.max_concurrent_requests}")
        logger.info(f"  Batch Size: {self.batch_size}")

    def setup_system_optimization(self):
        """Detect and optimize for AMD Ryzen system"""
        self.cpu_info = platform.processor()
        self.cpu_count = multiprocessing.cpu_count()
        self.memory_gb = round(psutil.virtual_memory().total / (1024**3))
        
        # AMD Ryzen specific optimizations
        if "AMD" in self.cpu_info and "Ryzen" in self.cpu_info:
            logger.info("AMD Ryzen processor detected - applying AMD optimizations")
            # AMD processors benefit from higher worker counts
            self.performance_multiplier = 1.5
        else:
            self.performance_multiplier = 1.0
            
        # High memory system optimizations
        if self.memory_gb >= 64:
            logger.info("High memory system (64GB+) - enabling memory-intensive optimizations")
            self.memory_intensive = True
            self.cache_size = 50000  # Large cache for high memory
        else:
            self.memory_intensive = False
            self.cache_size = 10000

    def setup_linux_optimizations(self):
        """Linux-specific performance optimizations"""
        try:
            # Set process priority for better performance
            os.nice(-5)  # Higher priority (requires appropriate permissions)
            logger.info("Process priority increased")
        except PermissionError:
            logger.info("Could not increase process priority (run as root for best performance)")
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        if self.session:
            asyncio.create_task(self.session.close())
        sys.exit(0)

    async def create_optimized_session(self):
        """Create aiohttp session optimized for AMD/Linux"""
        # AMD/Linux optimized connector settings
        connector = aiohttp.TCPConnector(
            limit=200,  # High connection pool for powerful system
            limit_per_host=50,  # High per-host limit
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
            # Linux-specific TCP optimizations
            socket_options=[
                (1, 6, 1),    # TCP_NODELAY
                (1, 9, 1),    # SO_KEEPALIVE
            ]
        )
        
        timeout = aiohttp.ClientTimeout(
            total=45,
            connect=15,
            sock_read=30
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        )

    async def scrape_url_batch(self, urls: List[str], semaphore: asyncio.Semaphore) -> List[str]:
        """Scrape a batch of URLs with AMD-optimized concurrency"""
        tasks = []
        for url in urls:
            task = asyncio.create_task(self.scrape_single_url(url, semaphore))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        job_urls = []
        for result in results:
            if isinstance(result, list):
                job_urls.extend(result)
            elif isinstance(result, Exception):
                logger.error(f"Batch error: {result}")
        
        return job_urls

    async def scrape_single_url(self, search_url: str, semaphore: asyncio.Semaphore) -> List[str]:
        """Scrape individual URL with error handling"""
        async with semaphore:
            try:
                params = {
                    'url': search_url,
                    'apikey': self.zenrows_api_key,
                    'js_render': 'true',
                    'wait': '3000',
                    'css_extractor': json.dumps({
                        "job_links": "a[href*='/companies/'][href*='/jobs/']"
                    })
                }
                
                async with self.session.get(self.base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        job_links = data.get('job_links', [])
                        
                        job_urls = []
                        for link in job_links:
                            href = link.get('href', '')
                            if href and '/jobs/' in href:
                                full_url = f"https://wellfound.com{href}" if href.startswith('/') else href
                                if full_url not in self.scraped_urls:
                                    job_urls.append(full_url)
                                    self.scraped_urls.add(full_url)
                        
                        if job_urls:
                            logger.info(f"Found {len(job_urls)} job URLs from {search_url}")
                        
                        # AMD-optimized rate limiting
                        await asyncio.sleep(self.rate_limit_delay)
                        return job_urls
                    
                    elif response.status == 429:
                        logger.warning(f"Rate limited for {search_url}, backing off...")
                        await asyncio.sleep(2.0)
                        return []
                    
                    else:
                        logger.error(f"HTTP {response.status} for {search_url}")
                        return []
                        
            except Exception as e:
                logger.error(f"Error scraping {search_url}: {e}")
                return []

    def load_search_urls(self) -> List[str]:
        """Load URLs from the generated wellfound URLs file"""
        urls_file = Path('wellfound_urls.json')
        if not urls_file.exists():
            logger.error("wellfound_urls.json not found. Run build_url.py first.")
            return []
        
        try:
            with open(urls_file, 'r') as f:
                data = json.load(f)
                urls = data.get('urls', [])
                logger.info(f"Loaded {len(urls)} search URLs")
                return urls
        except Exception as e:
            logger.error(f"Error loading URLs: {e}")
            return []

    def save_urls(self, urls: List[str]):
        """Save scraped URLs with performance stats"""
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True)
        
        output = {
            'urls': urls,
            'total_count': len(urls),
            'scraped_at': datetime.now().isoformat(),
            'system_info': {
                'cpu': self.cpu_info,
                'cpu_count': self.cpu_count,
                'memory_gb': self.memory_gb,
                'max_workers': self.max_workers,
                'performance_multiplier': self.performance_multiplier
            }
        }
        
        with open(data_dir / 'url.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        logger.info(f"Saved {len(urls)} URLs to data/url.json")

    async def run_scraping(self):
        """Main scraping orchestrator optimized for AMD Ryzen"""
        start_time = time.time()
        
        # Load search URLs
        search_urls = self.load_search_urls()
        if not search_urls:
            return
        
        logger.info(f"Starting AMD-optimized scraping of {len(search_urls)} search URLs")
        
        # Create optimized session
        await self.create_optimized_session()
        
        try:
            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            
            # Process URLs in optimized batches
            all_job_urls = []
            total_batches = (len(search_urls) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(search_urls), self.batch_size):
                batch_num = (i // self.batch_size) + 1
                batch = search_urls[i:i + self.batch_size]
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} URLs)")
                
                # Process batch with AMD-optimized concurrency
                batch_urls = await self.scrape_url_batch(batch, semaphore)
                all_job_urls.extend(batch_urls)
                
                # Progress reporting
                elapsed = time.time() - start_time
                avg_time_per_url = elapsed / (i + len(batch))
                remaining_urls = len(search_urls) - (i + len(batch))
                eta = remaining_urls * avg_time_per_url
                
                logger.info(f"Batch {batch_num} complete. "
                          f"Total URLs found: {len(all_job_urls)}. "
                          f"ETA: {eta/60:.1f} minutes")
                
                # Memory management for high-memory systems
                if len(all_job_urls) > self.cache_size:
                    # Save intermediate results
                    self.save_urls(all_job_urls)
                    logger.info(f"Saved intermediate results ({len(all_job_urls)} URLs)")
        
        finally:
            await self.session.close()
        
        # Final save
        self.save_urls(all_job_urls)
        
        # Performance summary
        total_time = time.time() - start_time
        urls_per_second = len(search_urls) / total_time
        
        logger.info("\n" + "="*60)
        logger.info("AMD RYZEN SCRAPING COMPLETE")
        logger.info("="*60)
        logger.info(f"Search URLs processed: {len(search_urls)}")
        logger.info(f"Job URLs found: {len(all_job_urls)}")
        logger.info(f"Total time: {total_time/60:.1f} minutes")
        logger.info(f"Processing speed: {urls_per_second:.1f} URLs/second")
        logger.info(f"System utilized: {self.cpu_count} cores, {self.memory_gb}GB RAM")
        logger.info("="*60)

async def main():
    """Main entry point"""
    # Verify environment
    if not os.getenv('ZENROWS_API_KEY'):
        logger.error("ZENROWS_API_KEY environment variable not set")
        logger.error("Please set it with: export ZENROWS_API_KEY='your_api_key'")
        return
    
    # System check
    cpu_count = multiprocessing.cpu_count()
    memory_gb = round(psutil.virtual_memory().total / (1024**3))
    
    logger.info(f"AMD Ryzen Linux Scraper Starting")
    logger.info(f"System: {cpu_count} cores, {memory_gb}GB RAM")
    
    # Optimize event loop for Linux
    if hasattr(asyncio, 'set_event_loop_policy'):
        if sys.platform.startswith('linux'):
            asyncio.set_event_loop_policy(asyncio.UnixSelectorEventLoopPolicy())
    
    scraper = AMDLinuxURLScraper()
    await scraper.run_scraping()

if __name__ == "__main__":
    asyncio.run(main()) 