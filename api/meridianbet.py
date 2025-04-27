# api/meridianbet.py

import os
import json
import requests
import time
from datetime import datetime
from seleniumwire import webdriver  # Import from seleniumwire
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException

# Constants
TARGET_PAGE_URL = "https://meridianbet.ba/sr/kladjenje/fudbal"
API_URL = "https://online.meridianbet.com/betshop/api/v1/standard/sport/58/leagues?page=0&time=ALL"
OUTPUT_DIR = os.path.join("data", "json", "meridianbet")
OUTPUT_FILE_TEMPLATE = "response_{timestamp}.json"
MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds

def extract_bearer_token(driver):
    """
    Extracts the Bearer token from intercepted network requests.
    
    Args:
        driver: The Selenium WebDriver instance.
    
    Returns:
        str or None: The extracted Bearer token if found, else None.
    """
    print("Extracting Bearer token from network requests...")
    for request in driver.requests:
        if request.response:
            if "https://online-ws2.meridianbet.com/betshop-online/" in request.url:
                # Parse the URL to extract the access_token parameter
                parsed_url = requests.utils.urlparse(request.url)
                query_params = requests.utils.parse_qs(parsed_url.query)
                access_token_list = query_params.get('access_token')
                if access_token_list:
                    bearer_token = access_token_list[0]
                    print("Bearer token successfully retrieved.")
                    return bearer_token
    print("Bearer token not found in the captured network requests.")
    return None

def make_api_request(bearer_token):
    """
    Makes a GET request to the API using the Bearer token.
    
    Args:
        bearer_token (str): The Bearer token for authorization.
    
    Returns:
        dict: The JSON response from the API or an error dictionary.
    """
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "sr",
        "Authorization": f"Bearer {bearer_token}",
        "Origin": "https://meridianbet.ba",
        "Referer": "https://meridianbet.ba/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/131.0.0.0 Safari/537.36",
        "Sec-CH-UA": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
    }

    try:
        print(f"Making API request to {API_URL} with Bearer token...")
        response = requests.get(API_URL, headers=headers, timeout=10)
        if response.status_code == 200:
            print("API request successful.")
            return response.json()
        elif response.status_code in [401, 403]:
            print(f"API request failed with status code {response.status_code}.")
            return {"error": "Unauthorized or Forbidden"}
        else:
            print(f"API request failed with status code {response.status_code}. Response: {response.text}")
            return {"error": f"Status Code: {response.status_code}"}
    except requests.exceptions.RequestException as e:
        print(f"RequestException occurred: {e}")
        return {"error": str(e)}
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError occurred: {e}")
        return {"error": "Invalid JSON response"}
    except Exception as e:
        print(f"An unexpected error occurred during API request: {e}")
        return {"error": str(e)}

def save_json_data(data):
    """
    Saves the JSON data to the specified output directory with a timestamp.
    
    Args:
        data (dict): The JSON data to save.
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = OUTPUT_FILE_TEMPLATE.format(timestamp=timestamp)
    file_path = os.path.join(OUTPUT_DIR, output_file)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Data successfully saved to {file_path}")
    except Exception as e:
        print(f"Failed to save data to {file_path}: {e}")

def main():
    """
    Main function to orchestrate the token retrieval and API data fetching.
    """
    retries = 0
    # Configure SeleniumWire options if needed
    seleniumwire_options = {
        # 'proxy': {
        #     'http': 'http://proxyserver:port',
        #     'https': 'https://proxyserver:port',
        #     'no_proxy': 'localhost,127.0.0.1'
        # }
    }
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode.
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Initialize seleniumwire webdriver
    driver = webdriver.Chrome(options=chrome_options, seleniumwire_options=seleniumwire_options)

    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        while retries < MAX_RETRIES:
            try:
                print(f"\nAttempt {retries + 1} of {MAX_RETRIES}")

                # Clear previous network requests
                driver.requests.clear()

                # Navigate to the target page
                print(f"Navigating to {TARGET_PAGE_URL}...")
                driver.get(TARGET_PAGE_URL)
                
                # Allow time for network requests to be captured
                time.sleep(5)  # Adjust as necessary based on network speed

                # Extract Bearer token from intercepted network requests
                bearer_token = extract_bearer_token(driver)
                if not bearer_token:
                    print("Bearer token not found. Retrying...")
                    retries += 1
                    time.sleep(RETRY_DELAY)
                    continue

                # Make API request with the extracted Bearer token
                api_response = make_api_request(bearer_token)
                if "error" not in api_response:
                    # Save the JSON data
                    save_json_data(api_response)
                    print("Data fetching and saving completed successfully.")
                    break  # Exit the loop after successful fetch
                elif api_response["error"] in ["Unauthorized or Forbidden"]:
                    print("Authentication error detected. Refreshing the page to obtain a new token...")
                    driver.refresh()
                    retries += 1
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"Encountered an error: {api_response['error']}. Retrying...")
                    retries += 1
                    time.sleep(RETRY_DELAY)

            except WebDriverException as e:
                print(f"WebDriverException occurred: {e}")
                retries += 1
                time.sleep(RETRY_DELAY)
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                retries += 1
                time.sleep(RETRY_DELAY)

        if retries >= MAX_RETRIES:
            print("Maximum retries reached. Exiting script.")

    finally:
        driver.quit()
        print("WebDriver session closed.")

if __name__ == "__main__":
    main()
