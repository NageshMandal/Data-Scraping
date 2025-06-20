#!/usr/bin/env python3
"""
Job Scraping Pipeline Main Controller
====================================

This script orchestrates the entire job scraping and classification pipeline:
1. URL Generation
2. URL Scraping  
3. Job Data Scraping
4. AI Classification
5. Elasticsearch Indexing

Usage:
    python main.py --step all              # Run complete pipeline
    python main.py --step generate_urls    # Generate URLs only
    python main.py --step scrape_urls      # Scrape job URLs only
    python main.py --step scrape_jobs      # Scrape job data only
    python main.py --step classify         # Classify jobs only
"""

import argparse
import logging
import sys
import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('job_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class JobScrapingPipeline:
    def __init__(self):
        self.mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        self.db_name = os.getenv("MONGO_DB_NAME", "job_scraping")
        self.client = None
        self.db = None
        
    def connect_to_db(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            # Test connection
            self.client.admin.command('ping')
            logger.info("✅ Connected to MongoDB successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            return False
    
    def check_environment(self):
        """Check if all required environment variables are set"""
        required_vars = ["MONGO_URI", "GROQ_API_KEY"]
        missing_vars = []
        
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"❌ Missing required environment variables: {missing_vars}")
            logger.error("Please check your .env file")
            return False
        
        logger.info("✅ Environment variables check passed")
        return True
    
    def get_pipeline_status(self):
        """Get current pipeline status"""
        try:
            stats = {
                "total_urls": self.db.job_urls.count_documents({}),
                "scraped_urls": self.db.job_urls.count_documents({"scraped": True}),
                "pending_urls": self.db.job_urls.count_documents({"scraped": False}),
                "total_jobs": self.db.jobs.count_documents({}),
                "classified_jobs": self.db.classified_jobs.count_documents({}),
            }
            
            logger.info("📊 Pipeline Status:")
            logger.info(f"   Total URLs: {stats['total_urls']}")
            logger.info(f"   Scraped URLs: {stats['scraped_urls']}")
            logger.info(f"   Pending URLs: {stats['pending_urls']}")
            logger.info(f"   Total Jobs: {stats['total_jobs']}")
            logger.info(f"   Classified Jobs: {stats['classified_jobs']}")
            
            return stats
        except Exception as e:
            logger.error(f"❌ Failed to get pipeline status: {e}")
            return None
    
    def generate_urls(self):
        """Generate job search URLs"""
        logger.info("🔗 Starting URL generation...")
        try:
            import build_url
            # Run the URL generation
            os.system("python build_url.py")
            logger.info("✅ URL generation completed")
            return True
        except Exception as e:
            logger.error(f"❌ URL generation failed: {e}")
            return False
    
    def scrape_urls(self):
        """Scrape job URLs from search pages"""
        logger.info("🕷️ Starting URL scraping...")
        try:
            import scrap_urls
            # This will run the URL scraping
            os.system("python scrap_urls.py")
            logger.info("✅ URL scraping completed")
            return True
        except Exception as e:
            logger.error(f"❌ URL scraping failed: {e}")
            return False
    
    def scrape_jobs(self):
        """Scrape detailed job data"""
        logger.info("📋 Starting job data scraping...")
        try:
            import scrap_jobData
            # This will run the job data scraping
            os.system("python scrap_jobData.py")
            logger.info("✅ Job data scraping completed")
            return True
        except Exception as e:
            logger.error(f"❌ Job data scraping failed: {e}")
            return False
    
    async def classify_jobs(self):
        """Classify jobs using AI"""
        logger.info("🤖 Starting job classification...")
        try:
            from data_classification import process_jobs
            await process_jobs(batch_size=int(os.getenv("BATCH_SIZE", 5)))
            logger.info("✅ Job classification completed")
            return True
        except Exception as e:
            logger.error(f"❌ Job classification failed: {e}")
            return False
    
    async def run_pipeline(self, step="all"):
        """Run the complete pipeline or specific step"""
        if not self.check_environment():
            return False
        
        if not self.connect_to_db():
            return False
        
        logger.info(f"🚀 Starting pipeline - Step: {step}")
        start_time = datetime.now()
        
        try:
            if step in ["all", "generate_urls"]:
                if not self.generate_urls():
                    return False
                if step == "generate_urls":
                    return True
            
            if step in ["all", "scrape_urls"]:
                if not self.scrape_urls():
                    return False
                if step == "scrape_urls":
                    return True
            
            if step in ["all", "scrape_jobs"]:
                if not self.scrape_jobs():
                    return False
                if step == "scrape_jobs":
                    return True
            
            if step in ["all", "classify"]:
                if not await self.classify_jobs():
                    return False
                if step == "classify":
                    return True
            
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"🎉 Pipeline completed successfully in {duration}")
            
            # Show final status
            self.get_pipeline_status()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return False
        finally:
            if self.client:
                self.client.close()

def main():
    parser = argparse.ArgumentParser(description="Job Scraping Pipeline Controller")
    parser.add_argument(
        "--step",
        choices=["all", "generate_urls", "scrape_urls", "scrape_jobs", "classify", "status"],
        default="all",
        help="Pipeline step to run (default: all)"
    )
    
    args = parser.parse_args()
    
    pipeline = JobScrapingPipeline()
    
    if args.step == "status":
        if pipeline.connect_to_db():
            pipeline.get_pipeline_status()
        return
    
    # Run the pipeline
    success = asyncio.run(pipeline.run_pipeline(args.step))
    
    if success:
        logger.info("✅ Pipeline execution completed successfully")
        sys.exit(0)
    else:
        logger.error("❌ Pipeline execution failed")
        sys.exit(1)

if __name__ == "__main__":
    main()