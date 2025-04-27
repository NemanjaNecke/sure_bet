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

# ---------------------------- Configuration ---------------------------- #

# Configure logging
LOG_DIR = 'log'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'scraper_sportplus_api.log'),
    filemode='w',
    level=logging.INFO,  # Change to logging.DEBUG for more verbosity
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# API Endpoint
API_URL = "https://www.sportplus.ba/api/v1/prematch/offer"

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
        ValueError: If JSON parsing fails.
    """
    session = requests.Session()
    session.headers.update(headers or {})
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Attempt {attempt} to fetch data from API.")
            response = session.get(url, timeout=60)  # Increased timeout for large responses
            response.raise_for_status()
            logging.info("Data fetched successfully from API.")

            # Attempt to parse the response as JSON
            try:
                response_json = response.json()
                # If the JSON is a string, parse it again
                if isinstance(response_json, str):
                    logging.info("Response JSON is a string. Parsing it as JSON again.")
                    data = json.loads(response_json)
                else:
                    data = response_json
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
    return {}

def map_odds(odds_list):
    """
    Maps the odds based on their alias without altering them.

    Args:
        odds_list (list): List of odds dictionaries.

    Returns:
        dict: Mapped odds dictionary with alias as keys.
    """
    mapped_odds = {}
    for odd in odds_list:
        try:
            alias = odd.get("alias", "").strip()
            value = odd.get("value", "N/A")
            if not alias:
                logging.warning(f"Odds entry without alias found: {odd}")
                continue

            # Use alias directly as key
            mapped_key = str(alias)
            if mapped_key == 'X':
                mapped_key = 'x'
            
            # Check for duplicates and handle accordingly
            if mapped_key in mapped_odds:
                # If key exists, append a suffix to differentiate
                suffix = 1
                new_key = f"{mapped_key}_{suffix}"
                while new_key in mapped_odds:
                    suffix += 1
                    new_key = f"{mapped_key}_{suffix}"
                mapped_key = new_key

            mapped_odds[mapped_key] = value
            logging.debug(f"Mapping Odds: {alias} -> {mapped_key}, Value: {value}")
        except Exception as e:
            logging.error(f"Error mapping odds: {e}")
            continue
    return mapped_odds

def convert_match_date(match_date_str):
    """
    Converts match date from ISO format to desired datetime format.

    Args:
        match_date_str (str): Match date in ISO format (e.g., "2024-12-30T19:30:00").

    Returns:
        datetime or str: Parsed datetime object or "N/A" if parsing fails.
    """
    try:
        match_dt = datetime.fromisoformat(match_date_str)
        logging.debug(f"Converted match date '{match_date_str}' to datetime object {match_dt}.")
        return match_dt
    except ValueError as e:
        logging.error(f"Error parsing match date '{match_date_str}': {e}")
        return "N/A"

def clean_team_name(team_name):
    """
    Cleans the team name by removing any content within parentheses and stripping whitespace.

    Args:
        team_name (str): Raw team name.

    Returns:
        str: Cleaned team name.
    """
    # Remove content within parentheses
    cleaned_name = re.sub(r'\s*\(.*?\)', '', team_name)
    # Replace any invalid characters with a placeholder or remove them
    cleaned_name = cleaned_name.encode('utf-8', errors='replace').decode('utf-8')
    # Strip leading/trailing whitespace
    cleaned_name = cleaned_name.strip()
    return cleaned_name

def split_competitors(competitors_str):
    """
    Splits the competitors string into home and away teams.

    Args:
        competitors_str (str): Competitors string (e.g., "Team A - Team B", "Team A-Team B", "Team A / Team B").

    Returns:
        tuple: (home_team, away_team)
    """
    # First, try splitting on ' - '
    if ' - ' in competitors_str:
        parts = competitors_str.split(' - ', 1)
    # If not found, try splitting on '-'
    elif '-' in competitors_str:
        parts = competitors_str.split('-', 1)
    # If not found, try splitting on '/'
    elif '/' in competitors_str:
        parts = competitors_str.split('/', 1)
    else:
        parts = [competitors_str]

    if len(parts) == 2:
        home = clean_team_name(parts[0])
        away = clean_team_name(parts[1])
    else:
        home = clean_team_name(parts[0])
        away = "N/A"

    return home, away

def process_matches(matches):
    """
    Processes the list of matches from the JSON response.

    Args:
        matches (list): List of match dictionaries.

    Returns:
        list: List of processed match data dictionaries.
    """
    processed_data = []
    for idx, match in enumerate(matches, start=1):
        try:
            # Ensure that match is a dictionary
            if not isinstance(match, dict):
                logging.warning(f"Match {idx} is not a dictionary. Skipping.")
                continue

            competitors = match.get("competitors", "N/A")
            home, away = split_competitors(competitors)

            match_date_str = match.get("matchDate", None)
            match_dt = convert_match_date(match_date_str) if match_date_str else "N/A"

            markets = match.get("markets", [])

            # Initialize match data
            match_data = {
                "home": home,
                "away": away,
                "kickoff": match_dt
            }

            # Iterate through each market and extract odds
            for market in markets:
                odds_list = market.get("odds", [])
                mapped_odds = map_odds(odds_list)
                for bet_type, value in mapped_odds.items():
                    # Avoid overwriting existing keys
                    if bet_type in match_data:
                        # If key exists, append with a suffix to differentiate
                        suffix = 1
                        new_key = f"{bet_type}_{suffix}"
                        while new_key in match_data:
                            suffix += 1
                            new_key = f"{bet_type}_{suffix}"
                        match_data[new_key] = value
                        logging.debug(f"Duplicate bet type '{bet_type}' found. Stored as '{new_key}'.")
                    else:
                        match_data[bet_type] = value

            # Only add match if 'home' and 'away' are present
            if home and away != "N/A":
                processed_data.append(match_data)
                logging.info(f"Processed match {idx}: {home} vs {away} at {match_dt}")
            else:
                logging.warning(f"Match {idx} has incomplete team information and was skipped: {match_data}")

        except Exception as e:
            logging.error(f"Error processing match {idx}: {e}")
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
            df['time'] = df['kickoff'].dt.strftime('%Y-%m-%d %H:%M:%S')
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

# ---------------------------- Main Scraper Function ---------------------------- #

def run():
    """
    Main function to execute the scraping process.
    """
    logging.info("Starting scraper_sportplus_api.")

    try:
        # Fetch data from API
        data = fetch_api_data(API_URL, headers=HEADERS, retries=3, backoff_factor=0.3)

        # Verify if 'matches' is present in the response
        matches = data.get("matches", [])
        if not matches:
            logging.warning("No matches found in the API response.")
            return

        logging.info(f"Found {len(matches)} matches in the API response.")

        # Process matches
        processed_matches = process_matches(matches)
        logging.info(f"Processed {len(processed_matches)} matches.")

        if not processed_matches:
            logging.warning("No matches were processed successfully.")
            return

        # Validate data
        validated_matches = validate_data(processed_matches)
        logging.info(f"Validated {len(validated_matches)} matches.")

        if not validated_matches:
            logging.warning("No valid matches were processed successfully.")
            return

        # Define paths for saving data
        excel_path = os.path.join("data", "sportplus.xlsx")
        pickle_path = os.path.join("pickle_data", "sportplus.pkl")

        # Save data to files
        save_data(validated_matches, excel_path, pickle_path)

    except ValueError as e:
        logging.error(f"JSON parsing error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the scraping process: {e}")

    finally:
        logging.info("Scraper_sportplus_api has finished execution.")

# ---------------------------- Execute the Script ---------------------------- #

if __name__ == "__main__":
    run()
