import os
import uuid
import datetime
import argparse
import openai
from pymongo import MongoClient, errors

# ——— Configuration ———
# OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    raise ValueError("Please set the OPENAI_API_KEY environment variable")

# MongoDB URI (defaults to localhost)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["chat_logs_db"]       # database name
col = db["chat_logs"]                   # collection name

def send_prompt(prompt: str, model: str = "gpt-3.5-turbo") -> str:
    """Send a single prompt to OpenAI and return the assistant’s reply."""
    messages = [{"role": "user", "content": prompt}]
    resp = openai.ChatCompletion.create(model=model, messages=messages)
    return resp.choices[0].message.content.strip()

def log_to_mongo(prompt: str, response: str):
    """Insert the prompt & response into MongoDB."""
    record = {
        "_id": str(uuid.uuid4()),
        "timestamp": datetime.datetime.utcnow(),
        "prompt": prompt,
        "response": response
    }
    try:
        col.insert_one(record)
        print(f"Logged to MongoDB with _id: {record['_id']}")
    except errors.PyMongoError as e:
        print(f"[MongoDB Error] Failed to insert record: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="Send a prompt to OpenAI and log the response to MongoDB."
    )
    parser.add_argument(
        "--prompt", "-p", required=True,
        help="The text prompt to send to OpenAI"
    )
    parser.add_argument(
        "--model", "-m", default="gpt-3.5-turbo",
        help="OpenAI model to use"
    )
    args = parser.parse_args()

    response = send_prompt(args.prompt, model=args.model)
    print("AI:", response)

    # Log into MongoDB
    log_to_mongo(args.prompt, response)

if __name__ == "__main__":
    main()
