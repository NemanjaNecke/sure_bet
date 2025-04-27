import logging
import os
import time
import pickle
import pandas as pd
import requests
from datetime import datetime
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException

# ---------------------------- Configuration ---------------------------- #

# Ensure the 'log' directory exists before configuring logging
os.makedirs("log", exist_ok=True)

# Configure logging
LOG_DIR = 'log'
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'scraper_betole_api.log'),
    filemode='w',
    level=logging.INFO,  # Change to logging.WARNING or logging.ERROR to reduce verbosity
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# API Endpoint
API_URL = "https://www.betole.ba/restapi/offer/ba/sport/S/mob?annex=0&desktopVersion=2.36.6.2&locale=ba"

# Headers (if required)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    # Add other headers if necessary
}

# ---------------------------- Helper Functions ---------------------------- #

def fetch_api_data(url, headers=None, retries=3, backoff_factor=0.3):
    """
    Fetches data from the given API endpoint with retries.

    Args:
        url (str): API endpoint URL.
        headers (dict): Optional headers for the request.
        retries (int): Number of retry attempts.
        backoff_factor (float): Backoff factor for retries.

    Returns:
        dict: JSON response from the API.

    Raises:
        HTTPError: If the request fails after retries.
    """
    session = requests.Session()
    session.headers.update(headers or {})
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Attempt {attempt} to fetch data from API.")
            response = session.get(url, timeout=10)
            response.raise_for_status()
            logging.info("Data fetched successfully from API.")
            return response.json()
        except (HTTPError, ConnectionError, Timeout) as e:
            logging.warning(f"Attempt {attempt} failed with error: {e}")
            if attempt == retries:
                logging.error("All retry attempts failed.")
                raise
            sleep_time = backoff_factor * (2 ** (attempt - 1))
            logging.info(f"Retrying after {sleep_time} seconds...")
            time.sleep(sleep_time)
        except RequestException as e:
            logging.error(f"Request failed: {e}")
            raise
    return {}

def map_odds(odds_dict):
    """
    Maps the odds based on provided rules:
    - '1' -> '1'
    - '2' -> 'x'
    - '3' -> '2'
    - Retains other odds as is.

    Args:
        odds_dict (dict): Original odds dictionary.

    Returns:
        dict: Mapped odds dictionary.
    """
    mapped_odds = {}
    for key, value in odds_dict.items():
        if key == "1":
            mapped_key = "1"
        elif key == "2":
            mapped_key = "x"
        elif key == "3":
            mapped_key = "2"
        else:
            # Retain other bet types with their original keys
            mapped_key = key
        mapped_odds[mapped_key] = value
        logging.debug(f"Mapping Odds: {key} -> {mapped_key}, Value: {value}")
    return mapped_odds

def convert_kickoff_time(kickoff_timestamp):
    """
    Converts kickoff time from Unix timestamp in milliseconds to datetime object.

    Args:
        kickoff_timestamp (int): Kickoff time in milliseconds.

    Returns:
        datetime: Parsed datetime object.
    """
    try:
        # Convert milliseconds to seconds
        kickoff_seconds = kickoff_timestamp / 1000
        kickoff_datetime = datetime.fromtimestamp(kickoff_seconds)
        logging.debug(f"Converted kickoff timestamp {kickoff_timestamp} to {kickoff_datetime}")
        return kickoff_datetime
    except Exception as e:
        logging.error(f"Error converting kickoff time {kickoff_timestamp}: {e}")
        return "N/A"

def process_matches(es_matches):
    """
    Processes the list of matches from the JSON response.

    Args:
        es_matches (list): List of match dictionaries.

    Returns:
        list: List of processed match data dictionaries.
    """
    processed_data = []
    for idx, match in enumerate(es_matches, start=1):
        try:
            home = match.get("home", "N/A")
            away = match.get("away", "N/A")
            kickoff_timestamp = match.get("kickOffTime", None)
            kickoff = convert_kickoff_time(kickoff_timestamp) if kickoff_timestamp else "N/A"
            odds_original = match.get("odds", {})
            odds_mapped = map_odds(odds_original)
            
            # Construct match data dictionary
            match_data = {
                "home": home,
                "away": away,
                "time": kickoff  # 'time' column instead of 'kickoff'
            }
            
            # Add mapped odds to match_data
            for bet_type, odd_value in odds_mapped.items():
                # Ensure that all odds are strings
                match_data[bet_type] = str(odd_value) if odd_value else "N/A"
            
            processed_data.append(match_data)
            logging.info(f"Processed match {idx}: {home} vs {away} at {kickoff}")
        except Exception as e:
            logging.error(f"Error processing match {idx}: {e}")
            continue
    return processed_data

def save_data(data, excel_path, pickle_path):
    """
    Saves the data to Excel and Pickle files.

    Args:
        data (list): List of match data dictionaries.
        excel_path (str): Path to save the Excel file.
        pickle_path (str): Path to save the Pickle file.
    """
    try:
        # Create DataFrame
        df = pd.DataFrame(data)
        logging.info(f"DataFrame created with {len(df)} records.")
        
        # Convert 'time' to desired format if it's a datetime object
        if not df.empty and isinstance(df.at[0, 'time'], datetime):
            df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            logging.info("Converted 'time' to 'YYYY-MM-DD HH:MM:SS' format.")
        
        # Ensure the data directory exists
        os.makedirs(os.path.dirname(excel_path), exist_ok=True)
        os.makedirs(os.path.dirname(pickle_path), exist_ok=True)
        
        # Save to Excel
        df.to_excel(excel_path, index=False)
        logging.info(f"Data saved to Excel at {excel_path}")
        
        # Save to Pickle
        with open(pickle_path, "wb") as f:
            pickle.dump(df, f)
        logging.info(f"Data saved to Pickle at {pickle_path}")
    except Exception as e:
        logging.error(f"Error saving data: {e}")

# ---------------------------- Main Scraper Function ---------------------------- #

def run():
    """
    Main function to execute the scraping process.
    """
    logging.info("Starting scraper_betole_api.")
    
    try:
        # Fetch data from API
        json_data = fetch_api_data(API_URL, headers=HEADERS, retries=3, backoff_factor=0.3)
        
        # Verify if 'esMatches' is present in the response
        es_matches = json_data.get("esMatches", [])
        if not es_matches:
            logging.warning("No matches found in the API response.")
            return
        
        logging.info(f"Found {len(es_matches)} matches in the API response.")
        
        # Process matches
        processed_matches = process_matches(es_matches)
        logging.info(f"Processed {len(processed_matches)} matches.")
        
        if not processed_matches:
            logging.warning("No matches were processed successfully.")
            return
        
        # Define paths for saving data
        excel_path = os.path.join("data", "betole_matches.xlsx")
        pickle_path = os.path.join("pickle_data", "betole_matches.pkl")
        
        # Save data to files
        save_data(processed_matches, excel_path, pickle_path)
        
    except Exception as e:
        logging.error(f"An unexpected error occurred in the scraping process: {e}")
    
    finally:
        logging.info("Scraper_betole_api has finished execution.")

# ---------------------------- Execute the Script ---------------------------- #

if __name__ == "__main__":
    run()
