#!/usr/bin/env python3
"""
Server-Optimized Job Scraping Pipeline
=====================================

Ultra high-performance job scraping pipeline optimized for professional server:
- 92GB RAM
- 16 cores / 32 threads AMD Ryzen 9
- High-speed NVMe storage
- Professional server-grade hardware
"""

import os
import sys
import json
import time
import argparse
import subprocess
import psutil
from typing import Dict, Any
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load server configuration
def load_server_config():
    config_path = os.path.join(os.path.dirname(__file__), "config", "server_config.json")
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Server config not found: {config_path}")
        return None

class ServerOptimizedJobScrapingPipeline:
    def __init__(self):
        # Load server configuration
        self.config = load_server_config()
        if not self.config:
            print("❌ Failed to load server configuration!")
            exit(1)
        
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
        
        # System monitoring
        self.initial_memory = psutil.virtual_memory().used / (1024**3)  # GB
        self.initial_cpu = psutil.cpu_percent()

    def validate_server_specs(self):
        """Validate that we're running on the expected server hardware"""
        print("🔍 Validating Server Specifications...")
        
        # Check RAM
        total_ram_gb = psutil.virtual_memory().total / (1024**3)
        expected_ram = self.config["server_specs"]["ram_gb"]
        
        if total_ram_gb < expected_ram * 0.9:  # Allow 10% variance
            print(f"⚠️ WARNING: Expected {expected_ram}GB RAM, found {total_ram_gb:.1f}GB")
        else:
            print(f"✅ RAM: {total_ram_gb:.1f}GB (Expected: {expected_ram}GB)")
        
        # Check CPU cores
        cpu_cores = psutil.cpu_count(logical=False)
        cpu_threads = psutil.cpu_count(logical=True)
        expected_cores = self.config["server_specs"]["cpu_cores"]
        expected_threads = self.config["server_specs"]["cpu_threads"]
        
        print(f"✅ CPU: {cpu_cores} cores / {cpu_threads} threads (Expected: {expected_cores}/{expected_threads})")
        
        # Check available memory
        available_gb = psutil.virtual_memory().available / (1024**3)
        if available_gb < 60:  # Need at least 60GB free for processing
            print(f"⚠️ WARNING: Only {available_gb:.1f}GB RAM available. Recommend freeing memory.")
        else:
            print(f"✅ Available RAM: {available_gb:.1f}GB")
        
        return True

    def connect_mongodb(self):
        """Connect to MongoDB with server-optimized settings"""
        try:
            db_config = self.config["database_config"]["mongodb"]
            
            # Server-optimized MongoDB connection
            self.client = MongoClient(
                self.mongo_uri,
                maxPoolSize=db_config["max_pool_size"],
                minPoolSize=db_config["connection_pool_size"],
                socketTimeoutMS=db_config["socket_timeout_ms"],
                connectTimeoutMS=db_config["connect_timeout_ms"],
                serverSelectionTimeoutMS=db_config["server_selection_timeout_ms"],
                writeConcern=db_config["write_concern"],
                readPreference=db_config["read_preference"]
            )
            
            # Test connection
            self.client.admin.command('ping')
            print("✅ Connected to MongoDB with server-optimized settings")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to MongoDB: {e}")
            return False

    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics with server metrics"""
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
            
            # Server resource usage
            current_memory = psutil.virtual_memory().used / (1024**3)  # GB
            memory_used = current_memory - self.initial_memory
            cpu_usage = psutil.cpu_percent(interval=1)
            available_memory = psutil.virtual_memory().available / (1024**3)
            
            return {
                "job_urls": job_urls_count,
                "job_data": job_data_count,
                "classified_jobs": classified_count,
                "scraped_jobs": scraped_count,
                "scrape_progress": scrape_progress,
                "classification_progress": classification_progress,
                "pipeline_runtime": time.time() - self.pipeline_start_time,
                "server_stats": {
                    "memory_used_gb": memory_used,
                    "cpu_usage": cpu_usage,
                    "available_memory_gb": available_memory,
                    "total_memory_gb": psutil.virtual_memory().total / (1024**3)
                }
            }
        except Exception as e:
            print(f"⚠️ Error getting stats: {e}")
            return {}

    def print_pipeline_status(self):
        """Print comprehensive pipeline status with server metrics"""
        stats = self.get_pipeline_stats()
        if not stats:
            return
            
        print(f"\n📊 SERVER-OPTIMIZED PIPELINE STATUS")
        print(f"=" * 60)
        print(f"🔗 Job URLs discovered: {stats['job_urls']:,}")
        print(f"📄 Job data scraped: {stats['job_data']:,} ({stats['scrape_progress']:.1f}%)")
        print(f"🤖 Jobs classified: {stats['classified_jobs']:,} ({stats['classification_progress']:.1f}%)")
        print(f"⏱️ Pipeline runtime: {stats['pipeline_runtime']/60:.1f} minutes")
        
        # Server performance metrics
        if 'server_stats' in stats:
            server = stats['server_stats']
            print(f"\n🔧 Server Performance:")
            print(f"   Memory used: {server['memory_used_gb']:.1f}GB / {server['total_memory_gb']:.1f}GB")
            print(f"   Available memory: {server['available_memory_gb']:.1f}GB")
            print(f"   CPU usage: {server['cpu_usage']:.1f}%")
        
        if self.step_times:
            print(f"\n⚡ Step Performance:")
            for step, duration in self.step_times.items():
                print(f"   {step}: {duration/60:.1f} minutes")
        print("=" * 60)

    def run_server_optimized_step(self, script_name: str, step_name: str) -> bool:
        """Run a server-optimized step with performance monitoring"""
        print(f"\n🚀 Starting server-optimized {step_name}...")
        step_start = time.time()
        
        # Set environment variables for server optimization
        env = os.environ.copy()
        env['SERVER_MODE'] = 'true'
        env['SERVER_CONFIG_PATH'] = os.path.join(os.path.dirname(__file__), "config", "server_config.json")
        
        try:
            # Run the server-optimized script
            result = subprocess.run([
                sys.executable, script_name
            ], capture_output=False, text=True, env=env)
            
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

    def scrape_urls_server_optimized(self) -> bool:
        """Run server-optimized URL scraping"""
        return self.run_server_optimized_step("scrap_urls_server_optimized.py", "URL Scraping")

    def scrape_jobs_server_optimized(self) -> bool:
        """Run server-optimized job data scraping"""
        return self.run_server_optimized_step("scrap_jobData_server_optimized.py", "Job Data Scraping")

    def classify_data_server_optimized(self) -> bool:
        """Run server-optimized AI classification"""
        return self.run_server_optimized_step("data_classification_server_optimized.py", "AI Classification")

    def index_data_server_optimized(self) -> bool:
        """Run server-optimized Elasticsearch indexing"""
        return self.run_server_optimized_step("index_classified_jobs.py", "Elasticsearch Indexing")

    def run_full_server_optimized_pipeline(self):
        """Run the complete server-optimized pipeline"""
        print("🚀 SERVER-OPTIMIZED JOB SCRAPING PIPELINE")
        print("🔥 MAXIMUM PERFORMANCE MODE - 92GB RAM / 32 THREADS")
        print("=" * 70)
        
        # Validate server specs
        if not self.validate_server_specs():
            return False
        
        # Connect to MongoDB
        if not self.connect_mongodb():
            return False
        
        # Initial status
        self.print_pipeline_status()
        
        # Step 1: Generate URLs
        if not self.generate_urls():
            print("❌ Pipeline failed at URL generation")
            return False
        
        # Step 2: Server-Optimized URL Scraping
        print(f"\n🕷️ Starting server-optimized URL scraping...")
        print(f"   • 40 concurrent workers (server-grade)")
        print(f"   • Smart rate limiting (25 req/sec)")
        print(f"   • Large batch operations (200 docs/batch)")
        print(f"   • 2GB memory buffer")
        
        if not self.scrape_urls_server_optimized():
            print("❌ Pipeline failed at URL scraping")
            return False
        
        # Step 3: Server-Optimized Job Data Scraping
        print(f"\n📄 Starting server-optimized job data scraping...")
        print(f"   • 35 concurrent workers")
        print(f"   • Enhanced rate limiting (20 req/sec)")
        print(f"   • Massive batch processing (150 docs/batch)")
        print(f"   • 4GB memory buffer")
        
        if not self.scrape_jobs_server_optimized():
            print("❌ Pipeline failed at job data scraping")
            return False
        
        # Step 4: Server-Optimized AI Classification
        print(f"\n🤖 Starting server-optimized AI classification...")
        print(f"   • 8 concurrent API calls")
        print(f"   • Optimized Groq rate limiting (4 req/sec)")
        print(f"   • Large batch processing (25 jobs/batch)")
        print(f"   • 8GB memory buffer")
        
        if not self.classify_data_server_optimized():
            print("❌ Pipeline failed at AI classification")
            return False
        
        # Step 5: Server-Optimized Elasticsearch Indexing
        print(f"\n🔍 Starting server-optimized Elasticsearch indexing...")
        print(f"   • 20 concurrent workers")
        print(f"   • Bulk indexing (1000 docs/bulk)")
        print(f"   • 4GB memory buffer")
        
        if not self.index_data_server_optimized():
            print("❌ Pipeline failed at Elasticsearch indexing")
            return False
        
        # Final status and performance report
        self.print_pipeline_status()
        
        total_time = time.time() - self.pipeline_start_time
        print(f"\n🎉 SERVER-OPTIMIZED PIPELINE COMPLETED!")
        print(f"⚡ Total execution time: {total_time/60:.1f} minutes")
        print(f"🔥 Ultra high-performance server configuration utilized")
        print(f"💪 Estimated 8-12x faster than M1 Pro configuration")
        
        return True

def main():
    parser = argparse.ArgumentParser(description='Server-Optimized Job Scraping Pipeline')
    parser.add_argument('--step', choices=[
        'generate_urls', 'scrape_urls', 'scrape_jobs', 
        'classify_data', 'index_data', 'full_pipeline'
    ], help='Run a specific step or full pipeline')
    parser.add_argument('--stats', action='store_true', help='Show pipeline statistics')
    parser.add_argument('--validate', action='store_true', help='Validate server specifications')
    
    args = parser.parse_args()
    
    pipeline = ServerOptimizedJobScrapingPipeline()
    
    if args.validate:
        pipeline.validate_server_specs()
        return
    
    if args.stats:
        if pipeline.connect_mongodb():
            pipeline.print_pipeline_status()
        return
    
    if not args.step:
        print("Please specify --step, --stats, or --validate")
        return
    
    # Connect to MongoDB first
    if not pipeline.connect_mongodb():
        return
    
    # Run the specified step
    success = False
    
    if args.step == 'generate_urls':
        success = pipeline.generate_urls()
    elif args.step == 'scrape_urls':
        success = pipeline.scrape_urls_server_optimized()
    elif args.step == 'scrape_jobs':
        success = pipeline.scrape_jobs_server_optimized()
    elif args.step == 'classify_data':
        success = pipeline.classify_data_server_optimized()
    elif args.step == 'index_data':
        success = pipeline.index_data_server_optimized()
    elif args.step == 'full_pipeline':
        success = pipeline.run_full_server_optimized_pipeline()
    
    if success:
        print(f"\n✅ Step '{args.step}' completed successfully!")
        pipeline.print_pipeline_status()
    else:
        print(f"\n❌ Step '{args.step}' failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 