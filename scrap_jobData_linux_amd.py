#!/usr/bin/env python3
"""
AMD Ryzen 9955HX Linux Optimized Job Data Scraper
Optimized for 16-core/32-thread AMD systems with 96GB+ RAM
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
from typing import List, Dict, Set, Optional
import multiprocessing
import signal
import sys
import gc
import resource

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('job_scraping_amd.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AMDLinuxJobScraper:
    def __init__(self):
        self.setup_system_optimization()
        self.zenrows_api_key = os.getenv('ZENROWS_API_KEY')
        if not self.zenrows_api_key:
            raise ValueError("ZENROWS_API_KEY environment variable is required")
        
        self.base_url = "https://api.zenrows.com/v1/"
        self.scraped_jobs = {}
        self.session = None
        
        # AMD Ryzen 9955HX optimized settings for job scraping
        self.max_workers = min(56, multiprocessing.cpu_count() * 3)  # Aggressive for job data
        self.max_concurrent_requests = 72  # Higher for job data scraping
        self.batch_size = 40  # Large batches for high memory
        self.rate_limit_delay = 0.25  # Faster rate for powerful system
        
        # Linux-specific optimizations
        self.setup_linux_optimizations()
        
        logger.info(f"AMD Ryzen Job Scraper initialized:")
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
            logger.info("AMD Ryzen processor detected - applying AMD-specific optimizations")
            self.performance_multiplier = 1.6  # Higher for job scraping
            # AMD benefits from aggressive threading for I/O bound tasks
            self.amd_optimized = True
        else:
            self.performance_multiplier = 1.0
            self.amd_optimized = False
            
        # Ultra-high memory system optimizations (96GB+)
        if self.memory_gb >= 90:
            logger.info("Ultra-high memory system (90GB+) - enabling aggressive caching")
            self.ultra_high_memory = True
            self.cache_size = 100000  # Huge cache for massive memory
            self.memory_buffer_size = 20000  # Large buffer
        elif self.memory_gb >= 64:
            logger.info("High memory system (64GB+) - enabling memory-intensive optimizations")
            self.ultra_high_memory = False
            self.cache_size = 50000
            self.memory_buffer_size = 10000
        else:
            self.ultra_high_memory = False
            self.cache_size = 10000
            self.memory_buffer_size = 5000

    def setup_linux_optimizations(self):
        """Linux-specific performance optimizations"""
        try:
            # Increase process priority
            os.nice(-10)  # Higher priority for job scraping
            logger.info("Process priority maximized")
        except PermissionError:
            logger.info("Could not increase process priority (run as root for optimal performance)")
        
        # Set memory limits for optimal performance
        try:
            # Increase memory limits for large datasets
            resource.setrlimit(resource.RLIMIT_AS, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            resource.setrlimit(resource.RLIMIT_DATA, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            logger.info("Memory limits optimized")
        except Exception as e:
            logger.info(f"Could not optimize memory limits: {e}")
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGUSR1, self._memory_stats_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self._save_progress()
        if self.session:
            asyncio.create_task(self.session.close())
        sys.exit(0)

    def _memory_stats_handler(self, signum, frame):
        """Handle memory stats signal (kill -USR1 <pid>)"""
        memory_info = psutil.virtual_memory()
        process_info = psutil.Process()
        
        logger.info(f"MEMORY STATS:")
        logger.info(f"  System: {memory_info.percent}% used ({memory_info.used/1024**3:.1f}GB/{memory_info.total/1024**3:.1f}GB)")
        logger.info(f"  Process: {process_info.memory_info().rss/1024**3:.1f}GB RSS")
        logger.info(f"  Jobs cached: {len(self.scraped_jobs)}")

    async def create_optimized_session(self):
        """Create aiohttp session optimized for AMD/Linux job scraping"""
        # Ultra-high performance connector for job data
        connector = aiohttp.TCPConnector(
            limit=300,  # Very high connection pool
            limit_per_host=75,  # High per-host limit
            ttl_dns_cache=600,  # Longer DNS cache
            use_dns_cache=True,
            keepalive_timeout=90,  # Longer keepalive
            enable_cleanup_closed=True,
            # Linux TCP optimizations for high throughput
            socket_options=[
                (1, 6, 1),    # TCP_NODELAY
                (1, 9, 1),    # SO_KEEPALIVE
                (6, 1, 1),    # TCP_CORK (Linux specific)
            ]
        )
        
        timeout = aiohttp.ClientTimeout(
            total=60,  # Longer timeout for job data
            connect=20,
            sock_read=40
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/html, */*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
            }
        )

    def extract_job_data(self, html_content: str, job_url: str) -> Optional[Dict]:
        """Extract comprehensive job data from HTML"""
        try:
            # Parse the HTML content for job information
            # This is a simplified extraction - in practice you'd use BeautifulSoup or similar
            
            job_data = {
                'url': job_url,
                'scraped_at': datetime.now().isoformat(),
                'title': self._extract_field(html_content, 'job-title'),
                'company': self._extract_field(html_content, 'company-name'),
                'location': self._extract_field(html_content, 'job-location'),
                'description': self._extract_field(html_content, 'job-description'),
                'requirements': self._extract_field(html_content, 'job-requirements'),
                'salary': self._extract_field(html_content, 'salary-range'),
                'job_type': self._extract_field(html_content, 'job-type'),
                'experience_level': self._extract_field(html_content, 'experience-level'),
                'skills': self._extract_skills(html_content),
                'company_info': self._extract_company_info(html_content),
                'benefits': self._extract_benefits(html_content)
            }
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data from {job_url}: {e}")
            return None

    def _extract_field(self, content: str, field_name: str) -> str:
        """Extract specific field from content"""
        # Simplified extraction logic
        # In practice, this would use proper HTML parsing
        import re
        
        patterns = {
            'job-title': r'<h1[^>]*class="[^"]*job-title[^"]*"[^>]*>([^<]+)</h1>',
            'company-name': r'<a[^>]*class="[^"]*company[^"]*"[^>]*>([^<]+)</a>',
            'job-location': r'<span[^>]*class="[^"]*location[^"]*"[^>]*>([^<]+)</span>',
            'job-description': r'<div[^>]*class="[^"]*description[^"]*"[^>]*>(.*?)</div>',
        }
        
        pattern = patterns.get(field_name, '')
        if pattern:
            match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return ""

    def _extract_skills(self, content: str) -> List[str]:
        """Extract skills from job content"""
        # Simplified skill extraction
        skills = []
        common_skills = [
            'Python', 'JavaScript', 'Java', 'C++', 'React', 'Node.js',
            'Docker', 'Kubernetes', 'AWS', 'Azure', 'GCP', 'SQL',
            'MongoDB', 'PostgreSQL', 'Redis', 'Git', 'Linux'
        ]
        
        content_lower = content.lower()
        for skill in common_skills:
            if skill.lower() in content_lower:
                skills.append(skill)
        
        return skills

    def _extract_company_info(self, content: str) -> Dict:
        """Extract company information"""
        return {
            'size': self._extract_field(content, 'company-size'),
            'industry': self._extract_field(content, 'industry'),
            'funding': self._extract_field(content, 'funding-stage'),
        }

    def _extract_benefits(self, content: str) -> List[str]:
        """Extract job benefits"""
        benefits = []
        common_benefits = [
            'Remote work', 'Health insurance', 'Dental insurance',
            'Stock options', 'Equity', 'Flexible hours', '401k',
            'Unlimited PTO', 'Gym membership', 'Learning budget'
        ]
        
        content_lower = content.lower()
        for benefit in common_benefits:
            if benefit.lower() in content_lower:
                benefits.append(benefit)
        
        return benefits

    async def scrape_job_batch(self, job_urls: List[str], semaphore: asyncio.Semaphore) -> Dict[str, Dict]:
        """Scrape a batch of job URLs with AMD-optimized concurrency"""
        tasks = []
        for url in job_urls:
            task = asyncio.create_task(self.scrape_single_job(url, semaphore))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        batch_jobs = {}
        for result in results:
            if isinstance(result, dict) and result:
                batch_jobs[result['url']] = result
            elif isinstance(result, Exception):
                logger.error(f"Batch error: {result}")
        
        return batch_jobs

    async def scrape_single_job(self, job_url: str, semaphore: asyncio.Semaphore) -> Optional[Dict]:
        """Scrape individual job with comprehensive error handling"""
        async with semaphore:
            try:
                params = {
                    'url': job_url,
                    'apikey': self.zenrows_api_key,
                    'js_render': 'true',
                    'wait': '4000',  # Longer wait for job pages
                    'premium_proxy': 'true',  # Use premium proxy for reliability
                }
                
                async with self.session.get(self.base_url, params=params) as response:
                    if response.status == 200:
                        html_content = await response.text()
                        
                        if len(html_content) > 1000:  # Valid job page
                            job_data = self.extract_job_data(html_content, job_url)
                            if job_data:
                                logger.info(f"Successfully scraped job: {job_data.get('title', 'Unknown')} at {job_data.get('company', 'Unknown')}")
                                
                                # AMD-optimized rate limiting
                                await asyncio.sleep(self.rate_limit_delay)
                                return job_data
                        else:
                            logger.warning(f"Invalid job page content for {job_url}")
                    
                    elif response.status == 429:
                        logger.warning(f"Rate limited for {job_url}, backing off...")
                        await asyncio.sleep(3.0)
                        return None
                    
                    elif response.status == 404:
                        logger.info(f"Job not found (404): {job_url}")
                        return None
                    
                    else:
                        logger.error(f"HTTP {response.status} for {job_url}")
                        return None
                        
            except asyncio.TimeoutError:
                logger.error(f"Timeout scraping {job_url}")
                return None
            except Exception as e:
                logger.error(f"Error scraping {job_url}: {e}")
                return None

    def load_job_urls(self) -> List[str]:
        """Load job URLs from the scraped URL file"""
        urls_file = Path('data/url.json')
        if not urls_file.exists():
            logger.error("data/url.json not found. Run URL scraping first.")
            return []
        
        try:
            with open(urls_file, 'r') as f:
                data = json.load(f)
                urls = data.get('urls', [])
                logger.info(f"Loaded {len(urls)} job URLs")
                return urls
        except Exception as e:
            logger.error(f"Error loading job URLs: {e}")
            return []

    def _save_progress(self):
        """Save current progress to disk"""
        if self.scraped_jobs:
            data_dir = Path('data')
            data_dir.mkdir(exist_ok=True)
            
            progress_file = data_dir / 'job_scraping_progress.json'
            with open(progress_file, 'w') as f:
                json.dump(self.scraped_jobs, f, indent=2)
            
            logger.info(f"Saved progress: {len(self.scraped_jobs)} jobs")

    def save_jobs(self, jobs: Dict[str, Dict]):
        """Save scraped jobs with performance stats"""
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True)
        
        output = {
            'jobs': jobs,
            'total_count': len(jobs),
            'scraped_at': datetime.now().isoformat(),
            'system_info': {
                'cpu': self.cpu_info,
                'cpu_count': self.cpu_count,
                'memory_gb': self.memory_gb,
                'max_workers': self.max_workers,
                'amd_optimized': self.amd_optimized,
                'ultra_high_memory': self.ultra_high_memory
            }
        }
        
        with open(data_dir / 'data.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        logger.info(f"Saved {len(jobs)} jobs to data/data.json")

    async def run_scraping(self):
        """Main job scraping orchestrator optimized for AMD Ryzen"""
        start_time = time.time()
        
        # Load job URLs
        job_urls = self.load_job_urls()
        if not job_urls:
            return
        
        logger.info(f"Starting AMD-optimized job scraping of {len(job_urls)} job URLs")
        logger.info(f"System: {self.cpu_count} cores, {self.memory_gb}GB RAM")
        
        # Create optimized session
        await self.create_optimized_session()
        
        try:
            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            
            # Process URLs in ultra-high performance batches
            all_jobs = {}
            total_batches = (len(job_urls) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(job_urls), self.batch_size):
                batch_num = (i // self.batch_size) + 1
                batch = job_urls[i:i + self.batch_size]
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} jobs)")
                
                # Process batch with AMD-optimized ultra-high concurrency
                batch_jobs = await self.scrape_job_batch(batch, semaphore)
                all_jobs.update(batch_jobs)
                self.scraped_jobs.update(batch_jobs)
                
                # Performance reporting
                elapsed = time.time() - start_time
                jobs_per_minute = len(all_jobs) / (elapsed / 60)
                remaining_jobs = len(job_urls) - (i + len(batch))
                eta_minutes = remaining_jobs / jobs_per_minute if jobs_per_minute > 0 else 0
                
                # Memory management
                memory_percent = psutil.virtual_memory().percent
                
                logger.info(f"Batch {batch_num} complete. "
                          f"Jobs scraped: {len(all_jobs)}/{len(job_urls)}. "
                          f"Speed: {jobs_per_minute:.1f} jobs/min. "
                          f"ETA: {eta_minutes:.1f} min. "
                          f"Memory: {memory_percent:.1f}%")
                
                # Save progress for ultra-high memory systems
                if batch_num % 10 == 0 or len(all_jobs) > self.memory_buffer_size:
                    self._save_progress()
                    
                    # Memory optimization for massive datasets
                    if self.ultra_high_memory and len(all_jobs) > self.cache_size:
                        logger.info("Running garbage collection for memory optimization")
                        gc.collect()
        
        finally:
            await self.session.close()
        
        # Final save
        self.save_jobs(all_jobs)
        
        # Performance summary
        total_time = time.time() - start_time
        jobs_per_hour = len(all_jobs) / (total_time / 3600)
        success_rate = len(all_jobs) / len(job_urls) * 100
        
        logger.info("\n" + "="*70)
        logger.info("AMD RYZEN JOB SCRAPING COMPLETE")
        logger.info("="*70)
        logger.info(f"Job URLs processed: {len(job_urls)}")
        logger.info(f"Jobs successfully scraped: {len(all_jobs)}")
        logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info(f"Total time: {total_time/60:.1f} minutes ({total_time/3600:.1f} hours)")
        logger.info(f"Processing speed: {jobs_per_hour:.1f} jobs/hour")
        logger.info(f"System utilization: {self.cpu_count} cores, {self.memory_gb}GB RAM")
        logger.info(f"AMD optimization: {'ENABLED' if self.amd_optimized else 'DISABLED'}")
        logger.info(f"Ultra-high memory mode: {'ENABLED' if self.ultra_high_memory else 'DISABLED'}")
        logger.info("="*70)

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
    
    logger.info(f"AMD Ryzen Linux Job Scraper Starting")
    logger.info(f"System: {cpu_count} cores, {memory_gb}GB RAM")
    
    # Optimize event loop for Linux high-performance
    if hasattr(asyncio, 'set_event_loop_policy'):
        if sys.platform.startswith('linux'):
            # Use the fastest event loop policy for Linux
            asyncio.set_event_loop_policy(asyncio.UnixSelectorEventLoopPolicy())
    
    scraper = AMDLinuxJobScraper()
    await scraper.run_scraping()

if __name__ == "__main__":
    asyncio.run(main()) 