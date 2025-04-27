import requests
import json
import pandas as pd
import logging
from datetime import datetime

# Configure Logging
logging.basicConfig(
    filename='log/mdshop.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Define Headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/utf8+json, application/json;q=0.9, text/plain;q=0.8, */*;q=0.7",
    "Referer": "https://mdshop.ba/",
    "Origin": "https://mdshop.ba",
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "sr-RS,sr;q=0.9,en-US;q=0.8,en;q=0.7",
    "language": "sr-Latn",
    "officeid": "1678",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Connection": "keep-alive"
}
def generate_api_url():
    """
    Generates the API URL with the current UTC date and time.
    :return: A string representing the API URL.
    """
    base_url = "https://mdoffer.mdshop.ba/api/offer/competitionsWithEventsStartingSoonForSportV2"
    sport_id = 25  # Assuming 25 represents 'Fudbal' (Football)
    region_id = 0   # Assuming 0 is a default or 'all regions'
    flag = False    # Based on your API structure
    current_time = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'  # Adding 'Z' for UTC
    page_number = 1
    event_mapping_types = [1, 2, 3, 4, 5]

    # Construct the path parameters
    path_params = f"{sport_id}/{region_id}/{str(flag).lower()}/{current_time}/{page_number}"

    # Construct query parameters
    query_params = "&".join([f"eventMappingTypes={et}" for et in event_mapping_types])

    # Combine to form the full URL
    api_url = f"{base_url}/{path_params}?{query_params}"

    logging.info(f"Generated API URL: {api_url}")
    return api_url
# Fetch Mapping Data
def fetch_mappings():
    mapping_url = "https://mdoffer.mdshop.ba/api/offer/webTree/null/true/true/true/2024-12-29T17:07:03.242/2029-12-29T17:06:33.000/false?eventMappingTypes=1&eventMappingTypes=2&eventMappingTypes=3&eventMappingTypes=4&eventMappingTypes=5"
    try:
        response = requests.get(mapping_url, headers=headers)
        response.raise_for_status()
        mapping_data = response.json()
        logging.info("Successfully fetched mapping data.")
        return mapping_data
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch mapping data: {e}")
        return None

# Parse and Store Mappings
def parse_mappings(mapping_data):
    """
    Parses the mapping data to create dictionaries for bet type names and outcome names.
    :param mapping_data: List containing mapping information.
    :return: Dictionaries for bet type names and outcome names.
    """
    bet_type_names = {}
    outcome_names = {}

    # Assuming each item in mapping_data contains 'betTypes' and 'outcomes'
    for item in mapping_data:
        # Parse bet types
        bet_types = item.get('betTypes', [])
        for bet_type in bet_types:
            bet_type_id = bet_type.get('id')
            bet_type_name = bet_type.get('name')
            if bet_type_id and bet_type_name:
                bet_type_names[bet_type_id] = bet_type_name

        # Parse outcomes
        outcomes = item.get('outcomes', [])
        for outcome in outcomes:
            outcome_id = outcome.get('id')
            outcome_name = outcome.get('name')
            if outcome_id and outcome_name:
                outcome_names[outcome_id] = outcome_name

    logging.info("Parsed bet type and outcome mappings.")
    return bet_type_names, outcome_names


# Apply Mappings to API Response
def apply_mappings_to_response(api_response, bet_type_names, outcome_names):
    structured_data = []
    competitions = api_response.get('competitions', [])
    for competition in competitions:
        competition_id = competition.get('competitionId')
        competition_name = competition.get('competitionName', 'Unknown Competition')
        events = competition.get('events', [])
        for event in events:
            event_id = event.get('id')
            event_name = event.get('name', 'Unknown Event')
            date_time = event.get('dateTime', 'Unknown DateTime')
            bets = event.get('bets', [])
            for bet in bets:
                bet_type_id = bet.get('betTypeId')
                bet_type_name = bet_type_names.get(bet_type_id, "Unknown Bet Type")
                bet_outcomes = bet.get('betOutcomes', [])
                for outcome in bet_outcomes:
                    bet_type_outcome_id = outcome.get('betTypeOutcomeId')
                    outcome_name = outcome_names.get(bet_type_outcome_id, "Unknown Outcome")
                    odd = outcome.get('odd', 'Unknown')
                    included_in_group = outcome.get('includedInGroup', False)
                    visibility_type_id = outcome.get('visibilityTypeId')
                    structured_data.append({
                        "Competition ID": competition_id,
                        "Competition Name": competition_name,
                        "Event ID": event_id,
                        "Event Name": event_name,
                        "DateTime": date_time,
                        "Bet Type ID": bet_type_id,
                        "Bet Type Name": bet_type_name,
                        "Outcome ID": bet_type_outcome_id,
                        "Outcome Name": outcome_name,
                        "Odd": odd,
                        "Included In Group": included_in_group,
                        "Visibility Type ID": visibility_type_id
                    })
    logging.info("Applied mappings to API response.")
    return structured_data
def get_api_data(api_url):
    """
    Fetches data from the API.
    :param api_url: The API endpoint URL.
    :return: JSON data from the API response.
    """
    logging.info(f"Sending GET request to API URL: {api_url}")
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        logging.info("API request successful.")

        # Save raw API response for debugging
        with open('raw_api_response.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info("Raw API response saved to 'raw_api_response.json'.")

        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None
    except ValueError:
        logging.error("Error parsing JSON response.")
        return None
# Main Function
def run():
    logging.info("=== Script Started ===")
    # Fetch and parse mappings
    mapping_data = fetch_mappings()
    if mapping_data:
        bet_type_names, outcome_names = parse_mappings(mapping_data)
        # Fetch API data
        api_url = generate_api_url()
        api_data = get_api_data(api_url)
        logging.info(f"API DATA {api_data}")
        if api_data:
            # Apply mappings to API response
            
            structured_data = apply_mappings_to_response(api_data, bet_type_names, outcome_names)
            if structured_data:
                # Convert to DataFrame and save
                df = pd.DataFrame(structured_data)
                df.to_excel("data/mdshop.xlsx", index=False)
                logging.info("Data saved to 'data/mdshop.xlsx'.")
            else:
                logging.error("No structured data to save.")
        else:
            logging.error("Failed to retrieve API data.")
    else:
        logging.error("Failed to retrieve mapping data.")
    logging.info("=== Script Finished ===")

if __name__ == "__main__":
    run()
