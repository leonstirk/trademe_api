import json
import boto3
import urllib.request
import urllib.error
import os
import datetime
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

S3_BUCKET = "tm-rental-listings"

# Search for all residential property listings
TRADEME_API_URL = "https://api.trademe.co.nz/v1/Search/Property/Rental.json"

# Load credentials from environment variables
CONSUMER_KEY = os.getenv('TRADEME_CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('TRADEME_CONSUMER_SECRET')

# Ensure correct OAuth signature format
HEADERS = {
    "Authorization": str(f"OAuth oauth_consumer_key={CONSUMER_KEY}, oauth_signature_method=PLAINTEXT, oauth_signature={CONSUMER_SECRET}%26")
}

# AWS S3 Client
s3_client = boto3.client("s3")

def fetch_page(url_page, max_retries=3, backoff_factor=2):
    """Fetch a single page with retries and detailed debugging"""
    attempt = 0
    while attempt < max_retries:
        try:
            logging.info(f"Fetching URL: {url_page}")
            req = urllib.request.Request(url_page, headers=HEADERS)

            with urllib.request.urlopen(req, timeout=10) as response:
                response_body = response.read().decode("utf-8")  # Convert bytes to string
                
                # Debugging: Log the first 500 characters of the response
                logging.debug(f"Raw API Response (first 500 chars): {response_body[:500]}")

                try:
                    data = json.loads(response_body)
                    if isinstance(data, dict):
                        return data
                    else:
                        logging.error(f"Unexpected response format: {type(data)} - {data}")
                except json.JSONDecodeError as e:
                    logging.error(f"JSON parsing error: {e}. Response was: {response_body[:500]}")
                    return None

        except urllib.error.HTTPError as e:
            logging.error(f"HTTP Error {e.code}: {e.read().decode()}")
        except urllib.error.URLError as e:
            logging.error(f"Network Error: {e.reason}")
        except Exception as e:
            logging.error(f"Unexpected Error: {e}")

        # Apply exponential backoff before retrying
        attempt += 1
        sleep_time = backoff_factor ** attempt
        logging.info(f"Retrying in {sleep_time} seconds... (Attempt {attempt}/{max_retries})")
        time.sleep(sleep_time)

    logging.error(f"Failed to fetch data after {max_retries} attempts.")
    return None

def fetch_and_store_trademe_listings():
    """Fetches each page separately and stores it in S3"""
    rows = 500  # Maximum rows per request
    page = 1
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    while True:
        url_page = f"{TRADEME_API_URL}?rows={rows}&page={page}"
        logging.info(f"Fetching page {page}...")

        data = fetch_page(url_page)
        if not data:
            logging.error(f"Skipping page {page} due to persistent failures.")
            break  # Stop pagination if request consistently fails

        listings = data.get("List", [])
        if not isinstance(listings, list):
            logging.error(f"Unexpected format in response. 'List' key missing or invalid: {data}")
            break  # Exit loop if response is malformed

        # Generate S3 file key for the page
        file_key = f"rental-listings/{today}/page_{page}.json"
        
        # Upload page to S3 immediately
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=file_key,
                Body=json.dumps(listings),
                ContentType="application/json"
            )
            logging.info(f"Successfully uploaded page {page} to S3: {file_key}")
        except Exception as e:
            logging.error(f"S3 upload failed for page {page}: {e}")
            return {"statusCode": 500, "body": f"S3 upload failed for page {page}: {e}"}

        total_count = data.get("TotalCount", 0)
        total_pages = -(-total_count // rows)  # Equivalent to math.ceil

        logging.info(f"Page {page}/{total_pages} retrieved and stored.")
                
        if page >= total_pages:
            break  # Stop when all pages are fetched

        page += 1

    logging.info(f"All pages fetched and stored successfully.")
    return {"statusCode": 200, "body": f"All pages stored in S3 under rental-listings/{today}/"}

def lambda_handler(event, context):
    logging.info("Lambda function started...")
    logging.info(f"Using TradeMe API URL: {TRADEME_API_URL}")
    
    result = fetch_and_store_trademe_listings()
    
    return result
