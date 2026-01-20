# HealthPartnersEval
Health Partners Python Eval for job
import requests
import pandas as pd
from pathlib import Path
import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# Configuration
BASE_URL = "https://data.cms.gov"
API_CATALOG_URL = f"{BASE_URL}/api/docs" # Base URL for API documentation/endpoints
TOPIC_SLUG = "hospitals"
DOWNLOAD_DIR = Path("./cms_hospital_data")
METADATA_FILE = Path("./download_metadata.json")

# Ensure download directory exists
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

def get_last_run_timestamp():
    """Reads the timestamp of the last successful run from the metadata file."""
    if METADATA_FILE.exists():
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
            # Return timestamp in format matching 'Last-Modified' header (RFC 1123)
            return metadata.get('last_run_timestamp')
    return None

def update_last_run_timestamp(timestamp):
    """Updates the metadata file with the current run timestamp."""
    with open(METADATA_FILE, 'w') as f:
        json.dump({'last_run_timestamp': timestamp}, f, indent=4)

def clean_column_name(col_name):
    """Converts a column name to snake_case."""
    # Replace spaces and special characters with underscores, convert to lowercase
    s = re.sub(r'[\s\W]+', '_', col_name)
    # Remove leading/trailing underscores and convert to lowercase
    return s.strip('_').lower()

def process_dataset(dataset_info, last_run_timestamp):
    """Downloads, processes, and saves a single CSV file if modified since last run."""
    dataset_id = dataset_info.get('id')
    download_url = dataset_info.get('downloadURL') # Direct download URL from the metadata
    if not download_url:
        print(f"No direct download URL found for dataset {dataset_id}. Skipping.")
        return False

    file_path = DOWNLOAD_DIR / f"{dataset_id}.csv"
    
    # Check if modified since last run using 'If-Modified-Since' header
    headers = {}
    if last_run_timestamp:
        headers['If-Modified-Since'] = last_run_timestamp

    try:
        response = requests.get(download_url, headers=headers, stream=True)
        # Check if file was modified (status 200) or not modified (status 304)
        if response.status_code == 304:
            print(f"Dataset {dataset_id} not modified since last run. Skipping download.")
            return False
        elif response.status_code == 200:
            print(f"Downloading and processing dataset {dataset_id}...")
            
            # Use pandas to read the CSV directly from the response stream
            df = pd.read_csv(response.content)
            
            # Convert column names to snake_case
            df.columns = [clean_column_name(col) for col in df.columns]
            
            # Save the processed CSV
            df.to_csv(file_path, index=False)
            print(f"Successfully processed and saved {file_path}")
            return True
        else:
            print(f"Failed to download {dataset_id}: Status code {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {dataset_id}: {e}")
        return False

def main():
    """Main function to orchestrate the dataset download process."""
    # Get the last run timestamp
    last_run_timestamp = get_last_run_timestamp()
    
    # Set the current run timestamp
    # Using a slightly past timestamp to ensure files modified *during* the run aren't missed next time
    current_run_timestamp = datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT")

    print(f"Last run timestamp: {last_run_timestamp if last_run_timestamp else 'None'}")
    print(f"Current run timestamp will be: {current_run_timestamp}")

    # Fetch list of datasets related to "Hospitals" theme
    # The API structure requires navigating to the topic page to find download links.
    # Programmatically we might need to search the API catalog for relevant datasets.
    # A search API call is used here for demonstration.
    # The actual API endpoint to find datasets by topic might be different; this is an estimate.
    
    # A robust approach would involve searching the API catalog for datasets under the "Hospitals" topic.
    # The API documentation suggests using search functionality on the catalog.
    # We can search for datasets where the 'topic' field contains "Hospitals".
    
    # This URL is an estimation based on general Socrata/DKAN API patterns for searching by topic/theme.
    # The actual field name for 'topic' might vary.
    search_url = f"{BASE_URL}/api/metastore/schemas/dataset/items?q=Hospitals&theme={TOPIC_SLUG}"
    
    try:
        response = requests.get(search_url)
        response.raise_for_status()
        datasets = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching dataset list: {e}")
        return

    if not datasets:
        print("No datasets found for the theme 'Hospitals' or an API error occurred.")
        return

    print(f"Found {len(datasets)} potential datasets related to 'Hospitals'.")

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all dataset processing tasks
        futures = [executor.submit(process_dataset, ds, last_run_timestamp) for ds in datasets]
        # Wait for all tasks to complete (optional, just to be sure)
        for future in futures:
            future.result()

    # Update the last run timestamp only if all went well (basic success assumption here)
    update_last_run_timestamp(current_run_timestamp)
    print("Download and processing complete. Metadata updated.")

if __name__ == "__main__":
    main()
