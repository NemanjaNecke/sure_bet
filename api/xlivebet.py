import logging
import os
import time
import pickle
import pandas as pd
import requests
from datetime import datetime
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import json
import re
import undetected_chromedriver as uc  # Importing undetected-chromedriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
    ElementClickInterceptedException,
    ElementNotInteractableException
)
from bs4 import BeautifulSoup

# ---------------------------- Configuration ---------------------------- #

# Configure logging
LOG_DIR = 'log'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'scraper_xlivebet_api.log'),
    filemode='w',
    level=logging.INFO,  # Change to logging.DEBUG for more verbosity
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# API Endpoints
BASE_URL = "https://sport.xlivebet.ba"
MAIN_PAGE_URL = f"{BASE_URL}/"
GET_CHAMPS_ENDPOINT = "/4a75140e-67a5-45ea-b3a0-cef362e61541/Prematch/GetChampsList"
GET_MATCHES_ENDPOINT = "/4a75140e-67a5-45ea-b3a0-cef362e61541/Prematch/GetMatchesList"

# Headers (to mimic a real browser)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json",
    "Referer": MAIN_PAGE_URL,
    "Origin": BASE_URL,
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
}

# ---------------------------- Helper Functions ---------------------------- #

def setup_selenium_driver():
    """
    Initializes the undetected-chromedriver with necessary options.
    Returns:
        uc.Chrome: The Selenium WebDriver instance.
    """
    try:
        options = uc.ChromeOptions()
        options.add_argument("--start-maximized")
        # Headless mode can sometimes be detected; consider running in headful mode
        # options.add_argument("--headless")  # Uncomment if headless is necessary
        options.add_argument("--disable-blink-features=AutomationControlled")
        # Additional stealth options can be added here

        driver = uc.Chrome(options=options)
        logging.info("Undetected WebDriver initialized successfully.")
        return driver
    except WebDriverException as e:
        logging.error(f"Error initializing undetected WebDriver: {e}", exc_info=True)
        return None

def fetch_guid(session, driver):
    """
    Fetches the GUID from the main page by parsing the HTML.
    
    Args:
        session (requests.Session): The requests session.
        driver (uc.Chrome): The Selenium WebDriver instance.
    
    Returns:
        str: The extracted GUID.
    """
    try:
        driver.get(MAIN_PAGE_URL)
        logging.info(f"Navigated to {MAIN_PAGE_URL}")
        time.sleep(5)  # Wait for the page to load and Cloudflare to pass

        # Extract cookies from Selenium and add them to the requests session
        selenium_cookies = driver.get_cookies()
        for cookie in selenium_cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        logging.info("Transferred cookies from Selenium to requests session.")

        # Make a GET request to the main page to ensure cookies are set
        response = session.get(MAIN_PAGE_URL, headers=HEADERS, timeout=60)
        response.raise_for_status()
        logging.info("Fetched main page successfully via requests session.")

        # Parse the HTML to extract GUID
        soup = BeautifulSoup(response.text, 'html.parser')
        # Adjust the extraction logic based on actual page structure
        # For example, if GUID is embedded in a JavaScript variable:
        match = re.search(r'/([a-f0-9\-]{36})/Prematch/GetChampsList', response.text)
        if match:
            guid = match.group(1)
            logging.info(f"Extracted GUID: {guid}")
            return guid
        else:
            # Alternative approach: Look for GUID in meta tags or other elements
            # This needs to be adjusted based on actual page content
            logging.error("GUID not found in the main page.")
            return None
    except Exception as e:
        logging.error(f"Error fetching GUID: {e}")
        return None

def fetch_api_data(session, url, method='POST', payload=None, headers=None, retries=3, backoff_factor=0.3):
    """
    Fetches data from the given API endpoint with retries.
    
    Args:
        session (requests.Session): The requests session.
        url (str): API endpoint URL.
        method (str): HTTP method ('GET' or 'POST').
        payload (dict): JSON payload for POST requests.
        headers (dict): Optional headers for the request.
        retries (int): Number of retry attempts.
        backoff_factor (float): Backoff factor for retries.
    
    Returns:
        dict or list: JSON response from the API.
    
    Raises:
        HTTPError: If the request fails after retries.
        ValueError: If JSON parsing fails.
    """
    headers = headers or {}
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Attempt {attempt} to fetch data from API: {url}")
            if method.upper() == 'POST':
                response = session.post(url, json=payload, headers=headers, timeout=60)
            else:
                response = session.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            logging.info("Data fetched successfully from API.")
            
            # Attempt to parse the response as JSON
            try:
                data = response.json()
                logging.info("JSON parsed successfully.")
                return data
            except json.JSONDecodeError as e:
                logging.error(f"JSON decoding failed: {e}")
                raise ValueError(f"JSON decoding failed: {e}")
        
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
    return None

def map_odds(stakes_list):
    """
    Maps the odds based on their bet type without altering them.
    
    Args:
        stakes_list (list): List of stakes dictionaries.
    
    Returns:
        dict: Mapped odds dictionary with bet type names as keys.
    """
    mapped_odds = {}
    for stake in stakes_list:
        try:
            bet_type = stake.get("N", "").strip()
            odds = stake.get("F", "N/A")
            if not bet_type:
                logging.warning(f"Stake entry without bet type found: {stake}")
                continue

            # Use bet type directly as key
            mapped_key = bet_type

            # Check for duplicates and handle accordingly
            if mapped_key in mapped_odds:
                # If key exists, append a suffix to differentiate
                suffix = 1
                new_key = f"{mapped_key}_{suffix}"
                while new_key in mapped_odds:
                    suffix += 1
                    new_key = f"{mapped_key}_{suffix}"
                mapped_key = new_key

            mapped_odds[mapped_key] = odds
            logging.debug(f"Mapping Odds: {bet_type} -> {mapped_key}, Odds: {odds}")
        except Exception as e:
            logging.error(f"Error mapping odds: {e}")
            continue
    return mapped_odds

def convert_match_date(match_date_str):
    """
    Converts match date from ISO format to desired datetime format.
    
    Args:
        match_date_str (str): Match date in ISO format (e.g., "2024-12-29T14:30:00Z").
    
    Returns:
        datetime or str: Parsed datetime object or "N/A" if parsing fails.
    """
    try:
        # Remove 'Z' if present and parse
        if match_date_str.endswith('Z'):
            match_date_str = match_date_str[:-1]
        match_dt = datetime.fromisoformat(match_date_str)
        logging.debug(f"Converted match date '{match_date_str}' to datetime object {match_dt}.")
        return match_dt
    except ValueError as e:
        logging.error(f"Error parsing match date '{match_date_str}': {e}")
        return "N/A"

def process_champions(champs):
    """
    Processes the list of championships to extract relevant information.
    
    Args:
        champs (list): List of championship dictionaries.
    
    Returns:
        list: List of processed championship data dictionaries.
    """
    processed_champs = []
    for idx, champ in enumerate(champs, start=1):
        try:
            champ_id = champ.get("Id")
            champ_name = champ.get("N", "").strip()
            if not champ_id or not champ_name:
                logging.warning(f"Championship {idx} has incomplete information and was skipped: {champ}")
                continue

            processed_champs.append({
                "champ_id": champ_id,
                "champ_name": champ_name
            })
            logging.info(f"Processed championship {idx}: {champ_name} (ID: {champ_id})")
        except Exception as e:
            logging.error(f"Error processing championship {idx}: {e}")
            continue
    return processed_champs

def process_matches(matches, champ_id, champ_name):
    """
    Processes the list of matches for a given championship.
    
    Args:
        matches (list): List of match dictionaries.
        champ_id (int): Championship ID.
        champ_name (str): Championship Name.
    
    Returns:
        list: List of processed match data dictionaries.
    """
    processed_data = []
    for idx, match in enumerate(matches, start=1):
        try:
            # Ensure that match is a dictionary
            if not isinstance(match, dict):
                logging.warning(f"Match {idx} in champ ID {champ_id} is not a dictionary. Skipping.")
                continue

            home = match.get("HT", "").strip()
            away = match.get("AT", "").strip()
            match_date_str = match.get("D", None)
            kickoff = convert_match_date(match_date_str) if match_date_str else "N/A"

            stake_types = match.get("StakeTypes", [])

            # Initialize match data
            match_data = {
                "champ_id": champ_id,
                "champ_name": champ_name,
                "home": home,
                "away": away,
                "kickoff": kickoff
            }

            # Iterate through each stake type and extract odds
            for stake_type in stake_types:
                stakes = stake_type.get("Stakes", [])
                mapped_odds = map_odds(stakes)
                for bet_type, odds in mapped_odds.items():
                    # Avoid overwriting existing keys
                    if bet_type in match_data:
                        # If key exists, append with a suffix to differentiate
                        suffix = 1
                        new_key = f"{bet_type}_{suffix}"
                        while new_key in match_data:
                            suffix += 1
                            new_key = f"{bet_type}_{suffix}"
                        match_data[new_key] = odds
                        logging.debug(f"Duplicate bet type '{bet_type}' found. Stored as '{new_key}'.")
                    else:
                        match_data[bet_type] = odds

            # Only add match if 'home' and 'away' are present
            if home and away != "N/A":
                processed_data.append(match_data)
                logging.info(f"Processed match {idx} in champ ID {champ_id}: {home} vs {away} at {kickoff}")
            else:
                logging.warning(f"Match {idx} in champ ID {champ_id} has incomplete team information and was skipped: {match_data}")

        except Exception as e:
            logging.error(f"Error processing match {idx} in champ ID {champ_id}: {e}")
            continue
    return processed_data

def validate_data(data):
    """
    Validates the processed data to ensure essential fields are present.
    
    Args:
        data (list): List of match data dictionaries.
    
    Returns:
        list: Validated and filtered list of match data dictionaries.
    """
    validated_data = []
    for match in data:
        if match.get("home") != "N/A" and match.get("away") != "N/A" and match.get("kickoff") != "N/A":
            validated_data.append(match)
        else:
            logging.warning(f"Invalid match data found and skipped: {match}")
    return validated_data

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

        # Convert 'kickoff' to desired format if it's a datetime object
        if not df.empty and isinstance(df.at[0, 'kickoff'], datetime):
            df['kickoff'] = df['kickoff'].dt.strftime('%Y-%m-%d %H:%M:%S')
            logging.info("Converted 'kickoff' to 'YYYY-MM-DD HH:MM:SS' format.")

        # Ensure the data directories exist
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

def transfer_cookies_to_session(session, driver):
    """
    Transfers cookies from Selenium driver to requests session.
    
    Args:
        session (requests.Session): The requests session.
        driver (uc.Chrome): The Selenium WebDriver instance.
    """
    try:
        selenium_cookies = driver.get_cookies()
        for cookie in selenium_cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        logging.info("Transferred cookies from Selenium to requests session.")
    except Exception as e:
        logging.error(f"Error transferring cookies: {e}")

# ---------------------------- Main Scraper Function ---------------------------- #

def run():
    """
    Main function to execute the scraping process.
    """
    logging.info("Starting scraper_xlivebet_api.")

    # Initialize Selenium WebDriver
    driver = setup_selenium_driver()
    if not driver:
        logging.error("Failed to initialize Selenium WebDriver. Exiting.")
        return

    # Initialize requests session
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        # Step 1: Fetch GUID using Selenium and transfer cookies to requests session
        guid = fetch_guid(session, driver)
        if not guid:
            logging.error("GUID extraction failed. Cannot proceed with API requests.")
            driver.quit()
            return

        # Step 2: Fetch Championship List using POST request
        champs_url = f"{BASE_URL}/{guid}{GET_CHAMPS_ENDPOINT}"
        champs_payload = {}  # Adjust payload if required
        champs_data = fetch_api_data(session, champs_url, method='POST', payload=champs_payload, headers=HEADERS)
        
        # Verify if champs_data is a list
        if not isinstance(champs_data, list):
            logging.error("Championship API response is not a list. Please check the API endpoint and response format.")
            driver.quit()
            return

        # Step 3: Process Championship List
        processed_champs = process_champions(champs_data)
        logging.info(f"Processed {len(processed_champs)} championships.")

        if not processed_champs:
            logging.warning("No championships were processed successfully.")
            driver.quit()
            return

        all_matches = []

        # Step 4: Fetch and Process Matches for Each Championship
        for champ in processed_champs:
            champ_id = champ["champ_id"]
            champ_name = champ["champ_name"]
            matches_url = f"{BASE_URL}/{guid}{GET_MATCHES_ENDPOINT}"
            matches_payload = {
                "ChampId": champ_id
                # Include other necessary fields if required by the API
            }
            matches = fetch_api_data(session, matches_url, method='POST', payload=matches_payload, headers=HEADERS)
            if not matches:
                logging.warning(f"No matches found for championship '{champ_name}' (ID: {champ_id}).")
                continue
            processed_matches = process_matches(matches, champ_id, champ_name)
            all_matches.extend(processed_matches)
            logging.info(f"Fetched and processed {len(processed_matches)} matches for championship '{champ_name}' (ID: {champ_id}).")

        logging.info(f"Total matches collected: {len(all_matches)}")

        if not all_matches:
            logging.warning("No matches were collected successfully.")
            driver.quit()
            return

        # Step 5: Validate Data
        validated_matches = validate_data(all_matches)
        logging.info(f"Validated {len(validated_matches)} matches.")

        if not validated_matches:
            logging.warning("No valid matches were processed successfully.")
            driver.quit()
            return

        # Step 6: Define Paths for Saving Data
        excel_path = os.path.join("data", "xlivebet_all_matches.xlsx")
        pickle_path = os.path.join("pickle_data", "xlivebet_all_matches.pkl")

        # Step 7: Save Data to Files
        save_data(validated_matches, excel_path, pickle_path)

    except ValueError as e:
        logging.error(f"JSON parsing error: {e}")
    except HTTPError as e:
        logging.error(f"HTTP error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the scraping process: {e}")

    finally:
        try:
            driver.quit()
            logging.info("Selenium WebDriver closed successfully.")
        except Exception as e:
            logging.error(f"Error closing Selenium WebDriver: {e}")
        logging.info("Scraper_xlivebet_api has finished execution.")

# ---------------------------- Execute the Script ---------------------------- #

if __name__ == "__main__":
    run()
