# Job Scraping, Classification, and Indexing Pipeline

This project implements an end-to-end pipeline to scrape job postings from Wellfound, classify them using OpenAI GPT-4, store structured data into MongoDB, and index it into Elasticsearch for advanced search and prospecting.

---

## 🔄 Full Flow Structure

### 1. Scrape Job Post URLs from Wellfound
- Go to [Wellfound](https://wellfound.com) (formerly AngelList Talent)
- Scrape job listing URLs based on selected `location` and/or `role`

### 2. Save Job URLs to MongoDB
- Store the scraped job URLs into the database (`job_scraping` -> `job_urls` collection)
- Each document includes URL, location, role, and a "scraped: false" flag

### 3. Scrape Full Job Post Data
- Fetch each saved job URL
- Scrape all available data:
  - Title, Description
  - Salary Range, Location, Experience Type
  - Company Info: Industries, Size, Funding, Founder, etc.
- Save full job post data to MongoDB (`job_scraping` -> `jobs` collection)

### 4. Save Full Job Post Data to MongoDB
- Store the entire structured job data (JSON format) including additional fields

### 5. Classify Job Post Data Using OpenAI
- Fetch raw job post data from `jobs` collection
- Send complete job data (excluding MongoDB `_id`) to OpenAI GPT-4
- Classify into structured JSON:
  - Categories
  - Focus Areas
  - Company Info
  - Job Info
  - Investment Signals
  - Summary

### 6. Save Classified Data to MongoDB
- Store the LLM-classified structured JSON into a new collection (`job_scraping` -> `classified_jobs`)

### 7. Send Classified Data to Elasticsearch
- Index classified job post into Elasticsearch (`job_classifications` index)
- Upsert or update company documents with new job postings

### 8. (Optional) Add Post-Processing Enhancements
- Score classified jobs with "signal_strength"
- Tag hot leads for outreach based on investment signals
- Set up triggers to notify prospecting teams or marketing automation

---

## 📊 Tech Stack

- Python
- MongoDB (job data storage)
- OpenAI GPT-4 (classification)
- Elasticsearch (searchable index)
- Asyncio + Tenacity (retries and batch processing)

---

---

> Built for high-scale prospecting and lead generation workflows. ✨