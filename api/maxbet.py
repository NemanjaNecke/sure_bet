import requests
import pandas as pd
import logging
from datetime import datetime
import time
import os

# ===============================
# 1. Configure Logging
# ===============================
# Ensure the 'log' directory exists
os.makedirs('log', exist_ok=True)

logging.basicConfig(
    filename='log/maxbet_scraper.log',  # Log file name
    filemode='a',                       # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO                  # Logging level
)

# ===============================
# 2. Define Headers
# ===============================
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/utf8+json, application/json;q=0.9, text/plain;q=0.8, */*;q=0.7",
    "Referer": "https://www.maxbet.ba/",
    "Origin": "https://www.maxbet.ba",
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "ba-BA,ba;q=0.9,en-US;q=0.8,en;q=0.7",
    "language": "ba-Latn",
    "officeid": "1678",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Connection": "keep-alive"
}

# ===============================
# 3. Generate API URLs
# ===============================
def generate_ttgg_lang_url():
    """
    Generates the URL for the first API endpoint (Bet Types Mapping).
    """
    base_url = "https://www.maxbet.ba/restapi/offer/ba/ttg_lang"
    params = {
        "mobileVersion": "1.2.1.2",
        "locale": "ba"
    }
    return f"{base_url}?mobileVersion={params['mobileVersion']}&locale={params['locale']}"

def generate_categories_url():
    """
    Generates the URL for the second API endpoint (Categories/Leagues).
    """
    base_url = "https://www.maxbet.ba/restapi/offer/ba/categories/sport/S/l"
    params = {
        "annex": "12",
        "desktopVersion": "1.2.1.2",
        "locale": "ba"
    }
    return f"{base_url}?annex={params['annex']}&desktopVersion={params['desktopVersion']}&locale={params['locale']}"

def generate_league_matches_url(league_id):
    """
    Generates the URL for the third API endpoint (League Matches) by inserting the league_id.
    """
    base_url = f"https://www.maxbet.ba/restapi/offer/ba/sport/S/league/{league_id}/mob"
    params = {
        "annex": "12",
        "desktopVersion": "1.2.1.2",
        "locale": "ba"
    }
    return f"{base_url}?annex={params['annex']}&desktopVersion={params['desktopVersion']}&locale={params['locale']}"

# ===============================
# 4. Fetching API Data
# ===============================
def fetch_api_data(url):
    """
    Fetches data from the given API URL.
    :param url: API endpoint URL.
    :return: JSON response as a Python dictionary.
    """
    logging.info(f"Fetching data from URL: {url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises HTTPError, if one occurred.
        data = response.json()
        logging.info(f"Data fetched successfully from {url}")
        return data
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred while fetching {url}: {http_err}")
    except Exception as err:
        logging.error(f"Other error occurred while fetching {url}: {err}")
    return None

# ===============================
# 5. Parsing and Mapping Data
# ===============================
def map_bet_types(bet_map):
    """
    Maps betTypeId to its details.
    :param bet_map: Dictionary containing betType mappings.
    :return: pandas DataFrame with betTypeId and details.
    """
    bet_types = []
    for bet_id, details in bet_map.items():
        bet_types.append({
            "Bet Type ID": bet_id,
            "Code": details.get("code"),
            "Caption": details.get("caption"),
            "Use Specifiers": details.get("useSpecifiers"),
            "Display Specifiers": details.get("displaySpecifiers"),
            "Sport": details.get("sport"),
            "Order Number": details.get("orderNumber")
        })
    df_bet_types = pd.DataFrame(bet_types)
    logging.info(f"Mapped {len(df_bet_types)} bet types.")
    return df_bet_types

def map_categories(categories):
    """
    Maps categories data.
    :param categories: List of category dictionaries.
    :return: pandas DataFrame with category details.
    """
    mapped_categories = []
    for category in categories:
        mapped_categories.append({
            "Category ID": category.get("id"),
            "Name": category.get("name"),
            "Image URL": category.get("imgUrl"),
            "Sport Code": category.get("url"),
            "Count": category.get("count"),
            "Type": category.get("type"),
            "Children": category.get("children")
        })
    df_categories = pd.DataFrame(mapped_categories)
    logging.info(f"Mapped {len(df_categories)} categories.")
    return df_categories

def merge_bet_types_with_categories(df_bet_types, df_categories):
    """
    Merges bet types with their corresponding categories based on Sport Code.
    :param df_bet_types: DataFrame containing bet types.
    :param df_categories: DataFrame containing categories.
    :return: Merged pandas DataFrame.
    """
    merged_df = pd.merge(
        df_bet_types,
        df_categories,
        left_on="Sport",
        right_on="Sport Code",
        how="left",
        suffixes=('_Bet', '_Category')
    )
    logging.info(f"Merged bet types with categories. Total records: {len(merged_df)}")
    return merged_df

def extract_matches(league_data):
    """
    Extracts match details and odds from league data.
    :param league_data: JSON data for a specific league.
    :return: List of dictionaries containing match and odds information.
    """
    matches = []
    league_id = league_data.get("id", "")
    league_name = league_data.get("name", "")
    es_matches = league_data.get("esMatches", [])

    for match in es_matches:
        match_id = match.get("id")
        match_code = match.get("matchCode")
        home_team = match.get("home")
        away_team = match.get("away")
        kick_off_time = match.get("kickOffTime")
        status = match.get("status")
        blocked = match.get("blocked")
        favourite = match.get("favourite")
        odds = match.get("odds", {})
        params = match.get("params", {})
        sport = match.get("sport", "")
        match_type = match.get("type", "")
        
        # Convert kick_off_time from milliseconds to readable datetime
        try:
            kick_off_datetime = datetime.fromtimestamp(kick_off_time / 1000)
        except:
            kick_off_datetime = None

        for odd_key, odd_value in odds.items():
            matches.append({
                "League ID": league_id,
                "League Name": league_name,
                "Match ID": match_id,
                "Match Code": match_code,
                "Home Team": home_team,
                "Away Team": away_team,
                "Kick Off Time": kick_off_datetime,
                "Status": status,
                "Blocked": blocked,
                "Favourite": favourite,
                "Sport": sport,
                "Match Type": match_type,
                "Odd Key": odd_key,
                "Odd Value": odd_value,
                "Params": params
            })

    return matches

def check_missing_bet_types(df_matches, df_bet_types):
    """
    Identifies and logs Odd Key's that are missing in betMap.
    """
    # Extract unique Odd Keys from matches
    odd_keys = df_matches['Odd Key'].astype(str).unique()
    
    # Extract all Codes from betMap
    bet_codes = df_bet_types['Code'].astype(str).unique()
    
    # Identify missing Odd Keys
    missing_keys = set(odd_keys) - set(bet_codes)
    
    if missing_keys:
        logging.warning(f"Missing Bet Types for Odd Keys: {missing_keys}")
        print(f"Warning: {len(missing_keys)} Bet Types are missing in betMap. Check logs for details.")
    else:
        logging.info("All Odd Keys are successfully mapped to Bet Types.")
        print("Success: All Odd Keys are mapped to Bet Types.")
    
    return missing_keys

def manual_mapping(df_matches):
    """
    Manually maps specific Odd Key's to Bet Type Names.
    """
    manual_mappings = {
        '55201': 'Extra Ining (DA/NE)',
        '55200': 'Extra Ining (DA/NE)',
        # Add more mappings as needed based on observed data
    }
    
    df_matches['Bet Type Name'] = df_matches['Odd Key'].astype(str).map(
        manual_mappings
    ).fillna(df_matches['Bet Type Name'])
    
    # Update "Unknown" where manual mapping still doesn't resolve
    unknown_bet_types = df_matches[df_matches['Bet Type Name'] == "Unknown"]['Odd Key'].unique()
    if len(unknown_bet_types) > 0:
        logging.warning(f"Still Unknown Odd Keys after manual mapping: {unknown_bet_types}")
    
    return df_matches

def log_unknown_odd_keys(df_matches):
    """
    Logs all unique Odd Key's labeled as "Unknown".
    """
    unknown_keys = df_matches[df_matches['Bet Type Name'] == "Unknown"]['Odd Key'].unique()
    if len(unknown_keys) > 0:
        with open('log/unknown_odd_keys.txt', 'w') as f:
            for key in unknown_keys:
                f.write(f"{key}\n")
        logging.info(f"Logged {len(unknown_keys)} unknown Odd Key's to 'log/unknown_odd_keys.txt'.")

def advanced_map_odds(df_matches, df_bet_types):
    """
    Attempts to map Odd Keys to Bet Type Names using pattern-based logic.
    """
    # Create a dictionary mapping 'Code' to 'Caption' from betMap
    code_to_caption = pd.Series(df_bet_types.Caption.values, index=df_bet_types.Code.astype(str)).to_dict()

    # Define a function to extract primary Bet Type ID
    def extract_primary_bet_type(odd_key):
        """
        Extracts the primary Bet Type ID from the Odd Key.
        Example: '55201' -> '55'
        Adjust the logic based on observed patterns.
        """
        # Example: Use the first two digits
        # Modify this based on actual data patterns
        if len(odd_key) >= 2:
            return odd_key[:2]
        else:
            return odd_key

    # Apply the extraction
    df_matches['Primary Bet Type ID'] = df_matches['Odd Key'].astype(str).apply(extract_primary_bet_type)
    
    # Map primary Bet Type ID to Bet Type Name
    df_matches['Bet Type Name'] = df_matches['Primary Bet Type ID'].map(code_to_caption)
    
    # Handle missing mappings
    unknown_bet_types = df_matches[df_matches['Bet Type Name'].isnull()]['Primary Bet Type ID'].unique()
    if len(unknown_bet_types) > 0:
        logging.warning(f"Unknown Primary Bet Type IDs: {unknown_bet_types}")
        df_matches['Bet Type Name'] = df_matches['Bet Type Name'].fillna("Unknown Primary Bet Type")
    
    return df_matches

# ===============================
# 6. Main Function
# ===============================
def main():
    logging.info("=== MaxBet Scraper Started ===")

    # Step 1: Generate URLs
    ttgg_lang_url = generate_ttgg_lang_url()
    categories_url = generate_categories_url()

    # Step 2: Fetch data from APIs
    bet_map_data = fetch_api_data(ttgg_lang_url)
    categories_data = fetch_api_data(categories_url)

    if not bet_map_data or not categories_data:
        logging.error("Failed to fetch necessary data from Bet Types or Categories APIs. Exiting script.")
        print("Failed to fetch necessary data. Check 'log/maxbet_scraper.log' for details.")
        return

    # Step 3: Map bet types
    df_bet_types = map_bet_types(bet_map_data.get("betMap", {}))

    # Step 4: Map categories
    df_categories = map_categories(categories_data.get("categories", []))

    # Step 5: Merge bet types with categories
    merged_df = merge_bet_types_with_categories(df_bet_types, df_categories)

    # Step 6: Identify unique leagues
    leagues = df_categories['Category ID'].tolist()
    logging.info(f"Found {len(leagues)} leagues to scrape.")

    all_matches = []

    # Step 7: Iterate through each league and fetch matches
    for idx, league_id in enumerate(leagues, start=1):
        league_url = generate_league_matches_url(league_id)
        league_data = fetch_api_data(league_url)
        if league_data and 'esMatches' in league_data:
            matches = extract_matches(league_data)
            all_matches.extend(matches)
            logging.info(f"Scraped {len(matches)} odds from League ID: {league_id} ({idx}/{len(leagues)})")
        else:
            logging.warning(f"No matches found or failed to fetch for League ID: {league_id} ({idx}/{len(leagues)})")
        
        # To prevent overwhelming the server, include a short delay
        time.sleep(0.5)  # 500 milliseconds

    if not all_matches:
        logging.error("No matches data fetched from any league. Exiting script.")
        print("No matches data fetched. Check 'log/maxbet_scraper.log' for details.")
        return

    # Step 8: Convert matches to DataFrame
    df_matches = pd.DataFrame(all_matches)
    logging.info(f"Total odds fetched: {len(df_matches)}")

    # Step 9: Map odds to bet type names
    df_matches_mapped = map_odds(df_matches, df_bet_types)

    # Step 10: Identify missing mappings
    missing_keys = check_missing_bet_types(df_matches_mapped, df_bet_types)

    # Step 11: Manual mapping for known Odd Keys
    df_matches_mapped = manual_mapping(df_matches_mapped)

    # Step 12: Log remaining unknown Odd Keys
    log_unknown_odd_keys(df_matches_mapped)

    # Step 13: Export data to Excel and Pickle
    # Ensure the 'data' and 'pickle_data' directories exist
    os.makedirs('data', exist_ok=True)
    os.makedirs('pickle_data', exist_ok=True)

    output_excel = "data/maxbet_scraper_output.xlsx"
    output_pickle = "pickle_data/maxbet_scraper_output.pkl"

    try:
        # Create Excel writer with multiple sheets
        with pd.ExcelWriter(output_excel, engine='xlsxwriter') as writer:
            df_bet_types.to_excel(writer, sheet_name='Bet Types', index=False)
            df_categories.to_excel(writer, sheet_name='Categories', index=False)
            df_matches_mapped.to_excel(writer, sheet_name='Matches', index=False)
        logging.info(f"Data exported successfully to {output_excel}")
        print(f"Data exported successfully to {output_excel}")
    except Exception as e:
        logging.error(f"Failed to export data to Excel: {e}")
        print(f"Failed to export data to Excel. Check 'log/maxbet_scraper.log' for details.")

    try:
        # Save the matches DataFrame to a pickle file
        df_matches_mapped.to_pickle(output_pickle)
        logging.info(f"Data exported successfully to {output_pickle}")
        print(f"Data exported successfully to {output_pickle}")
    except Exception as e:
        logging.error(f"Failed to export data to Pickle: {e}")
        print(f"Failed to export data to Pickle. Check 'log/maxbet_scraper.log' for details.")

    logging.info("=== MaxBet Scraper Finished ===")
    print("Scraping completed. Check the 'data' and 'pickle_data' directories for outputs.")

# ===============================
# 7. Execute the Script
# ===============================
if __name__ == "__main__":
    main()
