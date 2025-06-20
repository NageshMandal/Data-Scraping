#!/usr/bin/env python3
"""
Test Classification with Real Job Data
====================================

Test the improved classification system with the Weights & Biases example
to ensure it works correctly with the actual scraped data structure.
"""

import asyncio
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Test data - Weights & Biases example
test_job_data = {
    "company_name": "Weights & Biases",
    "hiring_stat": [
        "Actively Hiring",
        "Actively Hiring"
    ],
    "slogan": [
        "Developer Tools for Machine Learning"
    ],
    "position": "Staff Software Engineer, Metrics - US (Remote)",
    "price": "$210k – $290k",
    "location": "Remote",
    "experience_required": "Full Time",
    "visa": "Not Available",
    "remote_work_pol": None,
    "relocation": "Allowed",
    "skills": [],
    "job_description": "At Weights & Biases, our mission is to build the best tools for AI developers. We founded our company on the insight that while there were excellent tools for developers to build better code, there were no similarly great tools to help ML practitioners build better models. Starting with our first experiment tracking product, we have since expanded our solution into a comprehensive AI developer platform for organizations focused on building their own deep learning models and generative AI applications.\n\nWeights & Biases is a Series C company with $250M in funding and over 200 employees. We proudly serve over 1,000 customers and more than 30 foundation model builders including customers such as OpenAI, NVIDIA, Microsoft, and Toyota.\n\nAs a Staff Engineer, you'll lead the effort to scale our metrics and storage systems, ensuring they meet the complex demands of our most advanced customers. You'll play an instrumental role in the evolution of our platform as we grow our capability to ingest and query petabytes of data, making critical technical decisions that optimize the performance, reliability, and cost-effectiveness of our systems.\n\nYou will set the technical direction for the team, guiding the organization to balance short-term deliverables with strategic, long-term architectural improvements. You'll partner closely with product management, revenue teams, and other engineering groups to shape and deliver the future of W&B's flagship Models product, supporting experiment tracking and analytics utilized by over 2,500 leading machine learning and AI teams worldwide.\n\nResponsibilities:\n- Design and implement infrastructure that is scalable, efficient, and tailored to customer needs.\n- Lead the maintenance and monitoring of existing services, identifying and executing necessary improvements to ensure ongoing performance and reliability.\n- Participate in team-wide rotations to respond to customer support issues and site outages.\n- Communicate and collaborate effectively with internal and external stakeholders to achieve optimal outcomes.\n- Lead and mentor junior engineers, supporting their professional growth and development within the company.\n\nRequirements:\n- 8+ years of experience in software engineering, with a focus on data platforms and/or distributed systems.\n- Strong software engineering fundamentals and proficiency in at least one modern programming language (e.g., Python, Go, Typescript).\n- Extensive experience designing and scaling customer-facing APIs in production environments, ideally leveraging systems like MySQL, Postgres, Clickhouse, Bigtable, Pub/Sub, Kafka, etc.\n- Hands-on experience with Kubernetes, Terraform, and major cloud providers (e.g., GCP, AWS, Azure).\n\nOur benefits:\n- 🏝️ Flexible time off\n- 🩺 Medical, Dental, and Vision for employees and Family Coverage\n- 🏠 Remote first culture with in-office flexibility in San Francisco\n- 💵 Home office budget with a new high-powered laptop\n- 🥇 Truly competitive salary and equity\n- 🚼 12 weeks of Parental leave (U.S. specific)\n- 📈 401(k) (U.S. specific)\n- Supplemental benefits may be available depending on your location\n- Explore benefits by country\n\nWe encourage you to apply even if your experience doesn't perfectly align with the job description as we seek out diverse and creative perspectives. Team members who love to learn and collaborate in an inclusive environment will flourish with us. We are an equal opportunity employer and do not discriminate on the basis of race, religion, color, national origin, gender, sexual orientation, age, marital status, veteran status, or disability status. If you need additional accommodations to feel comfortable during your interview process, reach out at careers@wandb.com.\n\n#LI-Remote",
    "company_location": [
        "San Francisco"
    ],
    "company_size": [
        "201-500"
    ],
    "company_type": [
        "SaaS"
    ],
    "company_industries": [
        "Information Technology",
        "Information Services",
        "Machine Learning",
        "Artificial Intelligence",
        "Software",
        "Artificial Intelligence / Machine Learning",
        "B2B · SaaS · Mobile · Artificial Intelligence / Machine Learning"
    ],
    "additional_data": [],
    "perks": [
        "Healthcare benefits🩺 100% Medical, Dental, and Vision for employees and Family Coverage",
        "Retirement benefits🏦 401(k) to set you up for a future in saving for yourself",
        "Parental leave🚼 12 weeks of Parental leave",
        "Equity benefits📈 Meaningful Equity ownership in a $1,000,000,000 valued company with great multipliers",
        "Remote friendly🏠 Remote first culture with in-office flexibility in San Francisco",
        "Company meals🍏 Come visit us in SF and nosh on snacks and team meals together",
        "Pet-friendly office🐾 Take 'paws' and relax if you're in the SF office with one of our furry-finest"
    ],
    "amount_raised": "$250M",
    "founder": "Lukas Biewald",
    "source_url": "https://wellfound.com/company/wandb/jobs/test-job-url"
}

async def test_classification():
    """Test the classification function with real job data"""
    try:
        # Import the classification function
        from data_classification import classify_job_post
        
        print("🧪 Testing classification with Weights & Biases job data...")
        print(f"📊 Input data: {test_job_data['company_name']} - {test_job_data['position']}")
        
        # Run classification
        result = await classify_job_post(test_job_data)
        
        print("\n✅ Classification successful!")
        print("\n📋 Classification Result:")
        print("=" * 50)
        print(json.dumps(result, indent=2))
        
        # Validate structure
        required_keys = ['original_data', 'classification', 'prospecting_intel', 'keywords', 'summary']
        missing_keys = [key for key in required_keys if key not in result]
        
        if missing_keys:
            print(f"\n⚠️ Missing required keys: {missing_keys}")
        else:
            print(f"\n✅ All required keys present: {required_keys}")
            
        # Check original data preservation
        original = result.get('original_data', {})
        if original.get('company_name') == test_job_data['company_name']:
            print("✅ Original company name preserved correctly")
        else:
            print("❌ Original company name not preserved correctly")
            
        return True
        
    except Exception as e:
        print(f"❌ Classification test failed: {e}")
        return False

async def main():
    """Run the test"""
    print("🚀 Starting Classification Test")
    print("=" * 50)
    
    # Check if API key is available
    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        print("❌ GROQ_API_KEY not found in environment variables")
        print("Please check your .env file")
        return
    
    print("✅ GROQ_API_KEY found")
    
    # Run the test
    success = await test_classification()
    
    if success:
        print("\n🎉 Test completed successfully!")
        print("The improved classification system works with your data structure.")
    else:
        print("\n❌ Test failed!")
        print("Please check the error messages above.")

if __name__ == "__main__":
    asyncio.run(main()) 