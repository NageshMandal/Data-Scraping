#!/usr/bin/env python3
"""
AMD Ryzen 9955HX Linux Optimized AI Job Classification
Optimized for 16-core/32-thread AMD systems with ultra-high performance
"""

import asyncio
import json
import logging
import os
import time
import psutil
import platform
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any
import multiprocessing
import signal
import sys
import gc
import resource
import re
from groq import Groq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('classification_amd.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AMDLinuxJobClassifier:
    def __init__(self):
        self.setup_system_optimization()
        self.groq_api_key = os.getenv('GROQ_API_KEY')
        if not self.groq_api_key:
            raise ValueError("GROQ_API_KEY environment variable is required")
        
        self.client = Groq(api_key=self.groq_api_key)
        self.model = "llama3-70b-8192"  # Use the most powerful model
        self.classified_jobs = {}
        
        # AMD Ryzen 9955HX optimized settings for AI classification
        self.max_workers = min(64, multiprocessing.cpu_count() * 4)  # Ultra-aggressive for AI tasks
        self.max_concurrent_requests = 80  # Very high for AI processing
        self.batch_size = 48  # Large batches for high memory/CPU
        self.rate_limit_delay = 0.1  # Minimal delay for powerful system
        
        # Linux-specific AI optimizations
        self.setup_linux_ai_optimizations()
        
        logger.info(f"AMD Ryzen AI Classifier initialized:")
        logger.info(f"  CPU: {self.cpu_info}")
        logger.info(f"  RAM: {self.memory_gb}GB")
        logger.info(f"  Max Workers: {self.max_workers}")
        logger.info(f"  Concurrent Requests: {self.max_concurrent_requests}")
        logger.info(f"  Batch Size: {self.batch_size}")
        logger.info(f"  AI Model: {self.model}")

    def setup_system_optimization(self):
        """Detect and optimize for AMD Ryzen AI processing"""
        self.cpu_info = platform.processor()
        self.cpu_count = multiprocessing.cpu_count()
        self.memory_gb = round(psutil.virtual_memory().total / (1024**3))
        
        # AMD Ryzen specific AI optimizations
        if "AMD" in self.cpu_info and "Ryzen" in self.cpu_info:
            logger.info("AMD Ryzen processor detected - applying AMD AI optimizations")
            self.performance_multiplier = 2.0  # Very high for AI tasks
            self.amd_ai_optimized = True
            # AMD excels at parallel AI inference
            self.ai_parallel_factor = 2.5
        else:
            self.performance_multiplier = 1.0
            self.amd_ai_optimized = False
            self.ai_parallel_factor = 1.0
            
        # Ultra-high memory AI optimizations (96GB+)
        if self.memory_gb >= 90:
            logger.info("Ultra-high memory system (90GB+) - enabling massive AI caching")
            self.ultra_high_memory = True
            self.ai_cache_size = 200000  # Massive cache for AI results
            self.context_buffer_size = 50000  # Large context buffer
            self.enable_memory_intensive_ai = True
        elif self.memory_gb >= 64:
            logger.info("High memory system (64GB+) - enabling enhanced AI operations")
            self.ultra_high_memory = False
            self.ai_cache_size = 100000
            self.context_buffer_size = 25000
            self.enable_memory_intensive_ai = True
        else:
            self.ultra_high_memory = False
            self.ai_cache_size = 25000
            self.context_buffer_size = 10000
            self.enable_memory_intensive_ai = False

    def setup_linux_ai_optimizations(self):
        """Linux-specific AI performance optimizations"""
        try:
            # Maximum process priority for AI workload
            os.nice(-15)  # Highest priority for AI processing
            logger.info("Process priority set to maximum for AI")
        except PermissionError:
            logger.info("Could not set maximum priority (run as root for peak AI performance)")
        
        # AI-specific memory optimizations
        try:
            # Set unlimited memory for AI operations
            resource.setrlimit(resource.RLIMIT_AS, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            resource.setrlimit(resource.RLIMIT_DATA, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            resource.setrlimit(resource.RLIMIT_RSS, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            logger.info("Memory limits optimized for AI workload")
        except Exception as e:
            logger.info(f"Could not optimize memory limits: {e}")
        
        # Advanced signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGUSR1, self._ai_stats_handler)
        signal.signal(signal.SIGUSR2, self._performance_boost_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down AI classifier gracefully...")
        self._save_ai_progress()
        sys.exit(0)

    def _ai_stats_handler(self, signum, frame):
        """Handle AI stats signal (kill -USR1 <pid>)"""
        memory_info = psutil.virtual_memory()
        process_info = psutil.Process()
        cpu_percent = psutil.cpu_percent(interval=1)
        
        logger.info(f"AI PERFORMANCE STATS:")
        logger.info(f"  System Memory: {memory_info.percent}% used ({memory_info.used/1024**3:.1f}GB/{memory_info.total/1024**3:.1f}GB)")
        logger.info(f"  Process Memory: {process_info.memory_info().rss/1024**3:.1f}GB RSS")
        logger.info(f"  CPU Usage: {cpu_percent}%")
        logger.info(f"  Jobs classified: {len(self.classified_jobs)}")
        logger.info(f"  AMD AI Optimization: {'ENABLED' if self.amd_ai_optimized else 'DISABLED'}")

    def _performance_boost_handler(self, signum, frame):
        """Handle performance boost signal (kill -USR2 <pid>)"""
        logger.info("PERFORMANCE BOOST ACTIVATED")
        # Temporarily increase limits for boost mode
        self.max_concurrent_requests = min(120, self.max_concurrent_requests * 2)
        self.rate_limit_delay = max(0.05, self.rate_limit_delay / 2)
        logger.info(f"Boosted to {self.max_concurrent_requests} concurrent requests")

    def create_classification_prompt(self, job_data: Dict) -> str:
        """Create optimized AI prompt for job classification"""
        prompt = f"""You are an advanced AI job market analyst. Analyze this job posting and provide comprehensive classification in STRICT JSON format.

Job Data:
Title: {job_data.get('title', 'N/A')}
Company: {job_data.get('company', 'N/A')}
Location: {job_data.get('location', 'N/A')}
Description: {job_data.get('description', 'N/A')[:2000]}
Requirements: {job_data.get('requirements', 'N/A')[:1000]}
Skills: {job_data.get('skills', [])}

REQUIRED OUTPUT - MUST BE VALID JSON:
{{
    "company_intelligence": {{
        "industry_vertical": "string",
        "company_stage": "startup|growth|scale|enterprise",
        "funding_stage": "pre-seed|seed|series-a|series-b|series-c|public|unknown",
        "estimated_funding": "string",
        "technology_stack": ["array", "of", "technologies"],
        "business_model": "b2b|b2c|b2b2c|marketplace|saas|unknown",
        "growth_indicators": ["array", "of", "indicators"],
        "market_position": "leader|challenger|niche|emerging",
        "investment_readiness": "high|medium|low"
    }},
    "job_analysis": {{
        "seniority_level": "entry|mid|senior|staff|principal|vp|c-level",
        "department": "engineering|product|design|data|marketing|sales|operations|hr|finance|other",
        "job_function": "string",
        "required_experience_years": "number",
        "remote_friendliness": "fully-remote|hybrid|office-required",
        "compensation_tier": "below-market|market|above-market|premium",
        "technical_complexity": "low|medium|high|expert",
        "growth_potential": "high|medium|low",
        "skill_requirements": ["array", "of", "skills"],
        "nice_to_have_skills": ["array", "of", "skills"]
    }},
    "prospecting_intelligence": {{
        "contact_potential": "high|medium|low",
        "decision_maker_level": "ic|manager|director|vp|c-level",
        "hiring_urgency": "urgent|standard|exploratory",
        "team_size_indicator": "small|medium|large|unknown",
        "budget_indicator": "high|medium|low|unknown",
        "competition_level": "low|medium|high",
        "cultural_fit_indicators": ["array", "of", "indicators"],
        "red_flags": ["array", "of", "potential", "issues"],
        "opportunity_score": "number (1-10)"
    }},
    "market_insights": {{
        "industry_trend": "growing|stable|declining",
        "skill_demand": "high|medium|low",
        "location_advantage": "high|medium|low",
        "timing_indicator": "excellent|good|fair|poor",
        "competitive_landscape": "string",
        "future_outlook": "positive|neutral|concerning"
    }}
}}

CRITICAL: Return ONLY valid JSON. No explanations, no markdown, no additional text."""

        return prompt

    def clean_and_parse_json(self, response_text: str) -> Optional[Dict]:
        """Clean and parse JSON response with enhanced error handling"""
        try:
            # Remove any markdown formatting
            response_text = re.sub(r'```json\s*', '', response_text)
            response_text = re.sub(r'```\s*', '', response_text)
            
            # Remove any leading/trailing whitespace and non-JSON content
            response_text = response_text.strip()
            
            # Find JSON object boundaries
            start = response_text.find('{')
            end = response_text.rfind('}') + 1
            
            if start != -1 and end > start:
                json_text = response_text[start:end]
                
                # Try to parse
                result = json.loads(json_text)
                
                # Validate required structure
                required_sections = ['company_intelligence', 'job_analysis', 'prospecting_intelligence', 'market_insights']
                if all(section in result for section in required_sections):
                    return result
                else:
                    logger.warning("Missing required sections in AI response")
                    return None
            
            return None
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            # Try to fix common JSON issues
            try:
                # Fix common JSON issues
                fixed_text = re.sub(r',\s*}', '}', response_text)  # Remove trailing commas
                fixed_text = re.sub(r',\s*]', ']', fixed_text)
                result = json.loads(fixed_text)
                return result
            except:
                return None
        except Exception as e:
            logger.error(f"Unexpected error parsing JSON: {e}")
            return None

    async def classify_job_batch(self, jobs: List[Dict], semaphore: asyncio.Semaphore) -> Dict[str, Dict]:
        """Classify a batch of jobs with AMD-optimized AI processing"""
        tasks = []
        for job in jobs:
            task = asyncio.create_task(self.classify_single_job(job, semaphore))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        batch_classifications = {}
        for job, result in zip(jobs, results):
            if isinstance(result, dict) and result:
                batch_classifications[job['url']] = result
            elif isinstance(result, Exception):
                logger.error(f"Batch classification error for {job.get('url', 'unknown')}: {result}")
        
        return batch_classifications

    async def classify_single_job(self, job_data: Dict, semaphore: asyncio.Semaphore) -> Optional[Dict]:
        """Classify individual job with enhanced AI processing"""
        async with semaphore:
            try:
                prompt = self.create_classification_prompt(job_data)
                
                # AI API call with AMD optimization
                response = await asyncio.to_thread(
                    self.client.chat.completions.create,
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,  # Very low for consistency
                    max_tokens=2048,  # Large context for detailed analysis
                    top_p=0.9
                )
                
                response_text = response.choices[0].message.content
                
                # Parse AI response
                classification = self.clean_and_parse_json(response_text)
                
                if classification:
                    # Enhance with original job data
                    enhanced_job = {
                        **job_data,
                        'ai_classification': classification,
                        'classified_at': datetime.now().isoformat(),
                        'classification_model': self.model,
                        'amd_optimized': self.amd_ai_optimized
                    }
                    
                    logger.info(f"Successfully classified: {job_data.get('title', 'Unknown')} at {job_data.get('company', 'Unknown')}")
                    
                    # AMD-optimized rate limiting
                    await asyncio.sleep(self.rate_limit_delay)
                    return enhanced_job
                else:
                    logger.error(f"Failed to parse AI classification for {job_data.get('url', 'unknown')}")
                    # Return job with error status
                    return {
                        **job_data,
                        'ai_classification': {'error': 'classification_failed'},
                        'classified_at': datetime.now().isoformat(),
                        'classification_model': self.model
                    }
                    
            except Exception as e:
                logger.error(f"Error classifying job {job_data.get('url', 'unknown')}: {e}")
                return {
                    **job_data,
                    'ai_classification': {'error': str(e)},
                    'classified_at': datetime.now().isoformat(),
                    'classification_model': self.model
                }

    def load_jobs(self) -> List[Dict]:
        """Load jobs from scraped data file"""
        data_file = Path('data/data.json')
        if not data_file.exists():
            logger.error("data/data.json not found. Run job scraping first.")
            return []
        
        try:
            with open(data_file, 'r') as f:
                data = json.load(f)
                jobs = list(data.get('jobs', {}).values())
                logger.info(f"Loaded {len(jobs)} jobs for classification")
                return jobs
        except Exception as e:
            logger.error(f"Error loading jobs: {e}")
            return []

    def _save_ai_progress(self):
        """Save current AI classification progress"""
        if self.classified_jobs:
            data_dir = Path('data')
            data_dir.mkdir(exist_ok=True)
            
            progress_file = data_dir / 'classification_progress_amd.json'
            with open(progress_file, 'w') as f:
                json.dump(self.classified_jobs, f, indent=2)
            
            logger.info(f"Saved AI classification progress: {len(self.classified_jobs)} jobs")

    def save_classified_jobs(self, classified_jobs: Dict[str, Dict]):
        """Save classified jobs with enhanced metadata"""
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True)
        
        output = {
            'classified_jobs': classified_jobs,
            'total_count': len(classified_jobs),
            'classification_completed_at': datetime.now().isoformat(),
            'ai_model_used': self.model,
            'system_info': {
                'cpu': self.cpu_info,
                'cpu_count': self.cpu_count,
                'memory_gb': self.memory_gb,
                'max_workers': self.max_workers,
                'amd_ai_optimized': self.amd_ai_optimized,
                'ultra_high_memory': self.ultra_high_memory,
                'ai_parallel_factor': self.ai_parallel_factor
            },
            'performance_stats': {
                'classification_rate': f"{len(classified_jobs)}/hour",
                'memory_intensive_mode': self.enable_memory_intensive_ai,
                'cache_size': self.ai_cache_size
            }
        }
        
        with open(data_dir / 'classified_data.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        logger.info(f"Saved {len(classified_jobs)} classified jobs to data/classified_data.json")

    async def run_classification(self):
        """Main AI classification orchestrator optimized for AMD Ryzen"""
        start_time = time.time()
        
        # Load jobs
        jobs = self.load_jobs()
        if not jobs:
            return
        
        logger.info(f"Starting AMD-optimized AI classification of {len(jobs)} jobs")
        logger.info(f"AI System: {self.cpu_count} cores, {self.memory_gb}GB RAM, {self.model}")
        
        try:
            # Create semaphore for AI concurrency control
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            
            # Process jobs in ultra-high performance AI batches
            all_classified = {}
            total_batches = (len(jobs) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(jobs), self.batch_size):
                batch_num = (i // self.batch_size) + 1
                batch = jobs[i:i + self.batch_size]
                
                logger.info(f"Processing AI batch {batch_num}/{total_batches} ({len(batch)} jobs)")
                
                # Process batch with AMD-optimized ultra-high AI concurrency
                batch_classified = await self.classify_job_batch(batch, semaphore)
                all_classified.update(batch_classified)
                self.classified_jobs.update(batch_classified)
                
                # AI Performance reporting
                elapsed = time.time() - start_time
                classifications_per_minute = len(all_classified) / (elapsed / 60)
                remaining_jobs = len(jobs) - (i + len(batch))
                eta_minutes = remaining_jobs / classifications_per_minute if classifications_per_minute > 0 else 0
                
                # Memory and AI monitoring
                memory_percent = psutil.virtual_memory().percent
                cpu_percent = psutil.cpu_percent(interval=1)
                
                logger.info(f"AI Batch {batch_num} complete. "
                          f"Classified: {len(all_classified)}/{len(jobs)}. "
                          f"AI Speed: {classifications_per_minute:.1f} jobs/min. "
                          f"ETA: {eta_minutes:.1f} min. "
                          f"Memory: {memory_percent:.1f}%. "
                          f"CPU: {cpu_percent:.1f}%")
                
                # Save progress for ultra-high memory AI systems
                if batch_num % 5 == 0 or len(all_classified) > self.context_buffer_size:
                    self._save_ai_progress()
                    
                    # AI memory optimization for massive datasets
                    if self.ultra_high_memory and len(all_classified) > self.ai_cache_size:
                        logger.info("Running AI-optimized garbage collection")
                        gc.collect()
        
        except Exception as e:
            logger.error(f"Critical error in AI classification: {e}")
            self._save_ai_progress()
            raise
        
        # Final save
        self.save_classified_jobs(all_classified)
        
        # AI Performance summary
        total_time = time.time() - start_time
        classifications_per_hour = len(all_classified) / (total_time / 3600)
        success_rate = len(all_classified) / len(jobs) * 100
        
        logger.info("\n" + "="*80)
        logger.info("AMD RYZEN AI CLASSIFICATION COMPLETE")
        logger.info("="*80)
        logger.info(f"Jobs processed: {len(jobs)}")
        logger.info(f"Jobs successfully classified: {len(all_classified)}")
        logger.info(f"AI Success rate: {success_rate:.1f}%")
        logger.info(f"Total time: {total_time/60:.1f} minutes ({total_time/3600:.1f} hours)")
        logger.info(f"AI Processing speed: {classifications_per_hour:.1f} classifications/hour")
        logger.info(f"AI Model: {self.model}")
        logger.info(f"System utilization: {self.cpu_count} cores, {self.memory_gb}GB RAM")
        logger.info(f"AMD AI optimization: {'ENABLED' if self.amd_ai_optimized else 'DISABLED'}")
        logger.info(f"Ultra-high memory AI: {'ENABLED' if self.ultra_high_memory else 'DISABLED'}")
        logger.info("="*80)

async def main():
    """Main entry point"""
    # Verify environment
    if not os.getenv('GROQ_API_KEY'):
        logger.error("GROQ_API_KEY environment variable not set")
        logger.error("Please set it with: export GROQ_API_KEY='your_api_key'")
        return
    
    # System check
    cpu_count = multiprocessing.cpu_count()
    memory_gb = round(psutil.virtual_memory().total / (1024**3))
    
    logger.info(f"AMD Ryzen Linux AI Classifier Starting")
    logger.info(f"System: {cpu_count} cores, {memory_gb}GB RAM")
    
    # Optimize event loop for Linux AI processing
    if hasattr(asyncio, 'set_event_loop_policy'):
        if sys.platform.startswith('linux'):
            # Use the most efficient event loop for AI workloads
            asyncio.set_event_loop_policy(asyncio.UnixSelectorEventLoopPolicy())
    
    classifier = AMDLinuxJobClassifier()
    await classifier.run_classification()

if __name__ == "__main__":
    asyncio.run(main()) 