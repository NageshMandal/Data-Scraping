#!/usr/bin/env python3
"""
Optimized Job Scraping Pipeline
===============================

High-performance job scraping pipeline optimized for MacBook M1 Pro.
Uses concurrent processing, smart rate limiting, and batch operations.
"""

import os
import sys
import time
import argparse
import subprocess
from typing import Dict, Any
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class OptimizedJobScrapingPipeline:
    def __init__(self):
        # Validate environment variables
        self.mongo_uri = os.getenv("MONGO_URI")
        if not self.mongo_uri:
            print("❌ ERROR: MONGO_URI not found in environment variables!")
            exit(1)
            
        self.db_name = os.getenv("MONGO_DB_NAME", "job_scraping")
        self.client = None
        
        # Performance metrics
        self.pipeline_start_time = time.time()
        self.step_times = {}

    def connect_mongodb(self):
        """Connect to MongoDB and verify connection"""
        try:
            self.client = MongoClient(self.mongo_uri)
            # Test connection
            self.client.admin.command('ping')
            print("✅ Connected to MongoDB successfully")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to MongoDB: {e}")
            return False

    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics"""
        if not self.client:
            return {}
        
        try:
            db = self.client[self.db_name]
            
            # Collection counts
            job_urls_count = db["job_urls"].count_documents({})
            job_data_count = db["job_data"].count_documents({})
            classified_count = db["classified_jobs"].count_documents({})
            
            # Progress calculations
            scraped_count = db["job_urls"].count_documents({"scraped": True})
            scrape_progress = (scraped_count / job_urls_count * 100) if job_urls_count > 0 else 0
            classification_progress = (classified_count / job_data_count * 100) if job_data_count > 0 else 0
            
            return {
                "job_urls": job_urls_count,
                "job_data": job_data_count,
                "classified_jobs": classified_count,
                "scraped_jobs": scraped_count,
                "scrape_progress": scrape_progress,
                "classification_progress": classification_progress,
                "pipeline_runtime": time.time() - self.pipeline_start_time
            }
        except Exception as e:
            print(f"⚠️ Error getting stats: {e}")
            return {}

    def print_pipeline_status(self):
        """Print comprehensive pipeline status"""
        stats = self.get_pipeline_stats()
        if not stats:
            return
            
        print(f"\n📊 OPTIMIZED PIPELINE STATUS")
        print(f"=" * 50)
        print(f"🔗 Job URLs discovered: {stats['job_urls']:,}")
        print(f"📄 Job data scraped: {stats['job_data']:,} ({stats['scrape_progress']:.1f}%)")
        print(f"🤖 Jobs classified: {stats['classified_jobs']:,} ({stats['classification_progress']:.1f}%)")
        print(f"⏱️ Pipeline runtime: {stats['pipeline_runtime']/60:.1f} minutes")
        
        if self.step_times:
            print(f"\n⚡ Step Performance:")
            for step, duration in self.step_times.items():
                print(f"   {step}: {duration/60:.1f} minutes")
        print("=" * 50)

    def run_optimized_step(self, script_name: str, step_name: str) -> bool:
        """Run an optimized step and track performance"""
        print(f"\n🚀 Starting optimized {step_name}...")
        step_start = time.time()
        
        try:
            # Run the optimized script
            result = subprocess.run([
                sys.executable, script_name
            ], capture_output=False, text=True)
            
            step_duration = time.time() - step_start
            self.step_times[step_name] = step_duration
            
            if result.returncode == 0:
                print(f"✅ {step_name} completed in {step_duration/60:.1f} minutes")
                return True
            else:
                print(f"❌ {step_name} failed with return code {result.returncode}")
                return False
                
        except Exception as e:
            print(f"❌ Error running {step_name}: {e}")
            return False

    def generate_urls(self) -> bool:
        """Generate URLs using the existing build_url.py"""
        print("\n🔗 Starting URL generation...")
        step_start = time.time()
        
        try:
            result = subprocess.run([
                sys.executable, "build_url.py"
            ], capture_output=True, text=True)
            
            step_duration = time.time() - step_start
            self.step_times["URL Generation"] = step_duration
            
            if result.returncode == 0:
                print(f"✅ URL generation completed in {step_duration:.1f} seconds")
                print(result.stdout)
                return True
            else:
                print(f"❌ URL generation failed: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Error in URL generation: {e}")
            return False

    def scrape_urls_optimized(self) -> bool:
        """Run optimized URL scraping"""
        return self.run_optimized_step("scrap_urls_optimized.py", "URL Scraping")

    def scrape_jobs_optimized(self) -> bool:
        """Run optimized job data scraping"""
        return self.run_optimized_step("scrap_jobData_optimized.py", "Job Data Scraping")

    def classify_data_optimized(self) -> bool:
        """Run optimized AI classification"""
        return self.run_optimized_step("data_classification_optimized.py", "AI Classification")

    def index_data(self) -> bool:
        """Run Elasticsearch indexing"""
        return self.run_optimized_step("simple_index_jobs.py", "Elasticsearch Indexing")

    def run_full_optimized_pipeline(self):
        """Run the complete optimized pipeline"""
        print("🚀 OPTIMIZED JOB SCRAPING PIPELINE")
        print("Designed for MacBook M1 Pro - Maximum Performance")
        print("=" * 60)
        
        # Connect to MongoDB
        if not self.connect_mongodb():
            return False
        
        # Initial status
        self.print_pipeline_status()
        
        # Step 1: Generate URLs
        if not self.generate_urls():
            print("❌ Pipeline failed at URL generation")
            return False
        
        # Step 2: Scrape URLs (Optimized)
        print(f"\n🕷️ Starting optimized URL scraping...")
        print(f"   • 20 concurrent workers (ZenRows Startup)")
        print(f"   • Smart rate limiting (15 req/sec)")
        print(f"   • Batch MongoDB operations (100 docs/batch)")
        
        if not self.scrape_urls_optimized():
            print("❌ Pipeline failed at URL scraping")
            return False
        
        # Step 3: Scrape Job Data (Optimized)
        print(f"\n📄 Starting optimized job data scraping...")
        print(f"   • 15 concurrent workers")
        print(f"   • Enhanced rate limiting (12 req/sec)")
        print(f"   • Thread-local connections")
        
        if not self.scrape_jobs_optimized():
            print("❌ Pipeline failed at job data scraping")
            return False
        
        # Step 4: AI Classification (Optimized)
        print(f"\n🤖 Starting optimized AI classification...")
        print(f"   • 4 concurrent API calls")
        print(f"   • Smart Groq rate limiting")
        print(f"   • Enhanced error recovery")
        
        if not self.classify_data_optimized():
            print("❌ Pipeline failed at AI classification")
            return False
        
        # Step 5: Index to Elasticsearch
        if not self.index_data():
            print("❌ Pipeline failed at Elasticsearch indexing")
            return False
        
        # Final status and performance report
        self.print_pipeline_status()
        
        total_time = time.time() - self.pipeline_start_time
        print(f"\n🎉 OPTIMIZED PIPELINE COMPLETED!")
        print(f"⚡ Total execution time: {total_time/60:.1f} minutes")
        print(f"🚀 Performance optimized for M1 Pro architecture")
        
        return True

def main():
    parser = argparse.ArgumentParser(description='Optimized Job Scraping Pipeline')
    parser.add_argument('--step', choices=[
        'generate_urls', 'scrape_urls', 'scrape_jobs', 
        'classify_data', 'index_data', 'full_pipeline'
    ], help='Run a specific step or full pipeline')
    parser.add_argument('--stats', action='store_true', help='Show pipeline statistics')
    
    args = parser.parse_args()
    
    pipeline = OptimizedJobScrapingPipeline()
    
    if args.stats:
        if pipeline.connect_mongodb():
            pipeline.print_pipeline_status()
        return
    
    if not args.step:
        print("Please specify --step or --stats")
        return
    
    # Connect to MongoDB first
    if not pipeline.connect_mongodb():
        return
    
    # Run the specified step
    success = False
    
    if args.step == 'generate_urls':
        success = pipeline.generate_urls()
    elif args.step == 'scrape_urls':
        success = pipeline.scrape_urls_optimized()
    elif args.step == 'scrape_jobs':
        success = pipeline.scrape_jobs_optimized()
    elif args.step == 'classify_data':
        success = pipeline.classify_data_optimized()
    elif args.step == 'index_data':
        success = pipeline.index_data()
    elif args.step == 'full_pipeline':
        success = pipeline.run_full_optimized_pipeline()
    
    if success:
        print(f"\n✅ Step '{args.step}' completed successfully!")
        pipeline.print_pipeline_status()
    else:
        print(f"\n❌ Step '{args.step}' failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 