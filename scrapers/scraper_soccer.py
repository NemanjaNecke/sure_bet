from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime
import time
import pandas as pd
import pickle
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
import re
import logging
import os

def expand_shadow_element(driver, element):
    """
    Expand a shadow root element.

    Args:
        driver: Selenium WebDriver instance.
        element: WebElement representing the shadow host.

    Returns:
        WebElement representing the shadow root, or None if not found.
    """
    try:
        shadow_root = driver.execute_script('return arguments[0].shadowRoot', element)
        return shadow_root
    except WebDriverException:
        # Shadow root not found
        return None

def get_shadow_element_recursive(driver, root, selectors):
    """
    Recursively traverse shadow roots to find the desired element.

    Args:
        driver: Selenium WebDriver instance.
        root: Current root element to search within.
        selectors: List of CSS selectors representing the path to the desired element.

    Returns:
        WebElement if found, else None.
    """
    try:
        if not selectors:
            return root
        selector = selectors.pop(0)
        element = root.find_element(By.CSS_SELECTOR, selector)
        shadow_root = expand_shadow_element(driver, element)
        if shadow_root:
            return get_shadow_element_recursive(driver, shadow_root, selectors)
        else:
            return get_shadow_element_recursive(driver, element, selectors)
    except NoSuchElementException:
        logging.warning(f"Element with selector '{selector}' not found.")
        return None
    except Exception as e:
        logging.error(f"Error traversing shadow DOM: {e}")
        return None

def get_shadow_element(driver, selectors):
    """
    Traverse multiple shadow roots to find the desired element.

    Args:
        driver: Selenium WebDriver instance.
        selectors: List of CSS selectors representing the path to the desired element.

    Returns:
        WebElement if found, else None.
    """
    try:
        root = driver
        return get_shadow_element_recursive(driver, root, selectors.copy())
    except Exception as e:
        logging.error(f"Error traversing selectors '{' > '.join(selectors)}': {e}")
        return None

def parse_kickoff(kickoff_text):
    """
    Parse the kickoff text to extract date and time.
    Expected format:
        '12:00
        Sre
        25.12.'
    """
    try:
        # Split the kickoff_text by line breaks and strip whitespace
        parts = [part.strip() for part in kickoff_text.split('\n') if part.strip()]
        if len(parts) != 3:
            logging.warning(f"Unexpected kickoff format: '{kickoff_text}'")
            return "N/A"

        time_str, day_abbr, date_str = parts

        # Mapping of Bosnian day abbreviations to English
        day_mapping = {
            "Pon": "Mon",
            "Uto": "Tue",
            "Sri": "Wed",
            "Čet": "Thu",
            "Pet": "Fri",
            "Sub": "Sat",
            "Ned": "Sun",
            "Sre": "Wed"  # Based on your output
        }

        day_str_english = day_mapping.get(day_abbr, day_abbr)

        # Convert date from '25.12.' to '25 12'
        date_match = re.match(r'(\d{2})\.(\d{2})\.', date_str)
        if not date_match:
            logging.warning(f"Date does not match pattern: '{date_str}'")
            return "N/A"
        day, month = date_match.groups()

        # Assuming the current year; adjust if necessary
        current_year = datetime.datetime.now().year

        combined_str = f"{day_str_english} {day} {month} {current_year} {time_str}"
        logging.info(f"Combined kickoff string: '{combined_str}'")

        # Parse datetime
        match_dt = datetime.datetime.strptime(combined_str, "%a %d %m %Y %H:%M")
        return match_dt
    except Exception as e:
        logging.error(f"Error parsing kickoff information: {e}")
        return "N/A"

def parse_teams(team_elements):
    """
    Parse team elements to extract home and away names.

    Args:
        team_elements: List of WebElement containing team names.

    Returns:
        Tuple of (home_team, away_team)
    """
    try:
        teams = []
        for team in team_elements:
            team_text = team.text.strip()
            if team_text:
                teams.append(team_text)
            else:
                # Attempt to extract from alt attribute of images
                try:
                    img = team.find_element(By.TAG_NAME, "img")
                    team_text = img.get_attribute("alt").strip()
                    teams.append(team_text if team_text else "N/A")
                except NoSuchElementException:
                    teams.append("N/A")
        if len(teams) >= 2:
            return teams[0], teams[1]
        else:
            return "N/A", "N/A"
    except Exception as e:
        logging.error(f"Error parsing team names: {e}")
        return "N/A", "N/A"

def parse_odds(driver, ds_odds):
    """
    Parse ds-odd elements to extract odds.

    Args:
        driver: Selenium WebDriver instance.
        ds_odds: List of WebElement representing ds-odd elements.

    Returns:
        List of odds as strings.
    """
    odds = []
    for idx, ds_odd in enumerate(ds_odds, start=1):
        try:
            # Attempt to expand the Shadow Root
            shadow_root_ds_odd = expand_shadow_element(driver, ds_odd)
            if shadow_root_ds_odd:
                # Now find the span inside the Shadow Root
                span_odd = shadow_root_ds_odd.find_element(By.CSS_SELECTOR, "span.odd-btn--odd")
                odd_value = span_odd.text.strip()
            else:
                # If Shadow Root not found, try alternative approach
                # Directly find the span within ds-odd
                span_odd = ds_odd.find_element(By.CSS_SELECTOR, "span.odd-btn--odd")
                odd_value = span_odd.text.strip()

            # Handle cases where odd_value might be empty or invalid
            if not odd_value or odd_value == "-":
                odd_value = "N/A"
            odds.append(odd_value)
            logging.info(f"Extracted Odd {idx}: {odd_value}")
        except NoSuchElementException:
            odds.append("N/A")
            logging.warning(f"Odd element not found at index {idx}.")
        except Exception as e:
            odds.append("N/A")
            logging.error(f"Error extracting odd at index {idx}: {e}")
    return odds

def get_unique_match_key(match, index):
    """
    Generate a unique key for each match.

    Args:
        match: WebElement representing the match row.
        index: Integer representing the match's position.

    Returns:
        String representing the unique key.
    """
    try:
        # Attempt to extract a unique attribute (e.g., data-id)
        unique_id = match.get_attribute("data-id")
        if unique_id:
            return unique_id
    except Exception:
        pass

    # Fallback to using the match index combined with other attributes
    return f"match_{index}"

def run():
    # Target website URL
    web = "https://www.soccerbet.ba/ba/sportsko-kladjenje/fudbal/S"
    # **Configure Logging Inside the Run Function**
    log_dir = 'log'
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_dir, 'scraper_soccer.log'),
        filemode='w',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Starting scraper_soccer")
    # Initialize the Chrome driver with options
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    # Uncomment the following line to run in headless mode
    options.add_argument("--headless")
    # To prevent detection, you can add more options
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        logging.info("WebDriver initialized successfully.")
    except WebDriverException as e:
        logging.error(f"Error initializing WebDriver: {e}")
        return

    try:
        driver.get(web)
        logging.info(f"Navigated to {web}")

        # Implement explicit wait for match rows to load
        try:
            # Wait up to 15 seconds for the match rows to be present
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ion-item-sliding.prematch-top-desk--content"))
            )
            match_rows = driver.find_elements(By.CSS_SELECTOR, "ion-item-sliding.prematch-top-desk--content")
            logging.info(f"Found {len(match_rows)} match rows initially.")
        except TimeoutException:
            logging.error("Timed out waiting for match rows to load.")
            return

        if not match_rows:
            logging.error("No match rows found. Exiting.")
            return

        # Initialize a list to hold all match data
        all_matches = []
        processed_matches = set()  # To avoid duplicates

        # Define the path to the scrollable container inside Shadow DOM
        selectors = [
            "ion-content",               # Shadow Host 1
            "div.inner-scroll.scroll-y" # Target Element
        ]

        # Locate the scrollable container within Shadow DOM
        scroll_container = get_shadow_element(driver, selectors)
        if not scroll_container:
            logging.error("Scrollable container not found within Shadow DOM.")
            driver.quit()
            return
        logging.info("Scrollable container found within Shadow DOM.")

        # Initialize variables for scrolling
        scroll_attempts = 0
        max_scroll_attempts = 20
        target_match_count = 500
        last_height = driver.execute_script("return arguments[0].scrollHeight;", scroll_container)
        
        while len(all_matches) < target_match_count and scroll_attempts < max_scroll_attempts:
            # Scroll down by a specific amount
            try:
                driver.execute_script("arguments[0].scrollTop += 500;", scroll_container)  # Scroll down by 500 pixels
                logging.info(f"Scrolled down by 500 pixels. Attempt {scroll_attempts + 1}")
                time.sleep(2)  # Wait for new matches to load
            except Exception as e:
                logging.error(f"Error during incremental scrolling: {e}")
                break

            # Check if new matches have been loaded
            new_height = driver.execute_script("return arguments[0].scrollHeight;", scroll_container)
            if new_height == last_height:
                # No new content loaded
                scroll_attempts += 1
                logging.info(f"No new matches loaded. Scroll attempt {scroll_attempts}/{max_scroll_attempts}")
                if scroll_attempts >= max_scroll_attempts:
                    logging.info("Maximum scroll attempts reached. Assuming all matches are loaded.")
                    break
            else:
                last_height = new_height
                scroll_attempts = 0  # Reset scroll attempts if new content is loaded

            # Re-fetch all current rows after scrolling
            try:
                rows = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "ion-item-sliding.prematch-top-desk--content")
                    )
                )
                current_row_count = len(rows)
                logging.info(f"Detected {current_row_count} rows after scrolling.")
            except TimeoutException:
                logging.warning("Timeout while waiting for rows to load after scrolling.")
                continue

            # Iterate through each row
            for idx, match in enumerate(rows, start=1):
                if len(all_matches) >= target_match_count:
                    break  # Stop if target is reached

                # Generate a unique key for the match
                unique_key = get_unique_match_key(match, idx)
                if unique_key in processed_matches:
                    continue  # Skip already processed matches

                # Mark as processed
                processed_matches.add(unique_key)

                # Scroll the current match into view
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", match)
                    time.sleep(0.5)  # Small delay to allow any dynamic content to load
                except Exception as e:
                    logging.error(f"Error scrolling match into view: {e}")

                logging.info(f"\nProcessing match {len(all_matches)+1}/{target_match_count}")
                match_data = {
                    "Kickoff": "N/A",
                    "Home": "N/A",
                    "Away": "N/A",
                    "Odd 1": "N/A",
                    "Odd 2": "N/A",
                    "Odd 3": "N/A",
                    "Odd 4": "N/A",
                    "Odd 5": "N/A",
                    "Odd 6": "N/A",
                    "Odd 7": "N/A",
                    "Odd 8": "N/A",
                    "Odd 9": "N/A"
                }

                # Extract Kickoff Time and Date
                try:
                    kickoff_el = match.find_element(By.CSS_SELECTOR, "div.es-match-kickoff")
                    kickoff_text = kickoff_el.text.strip()
                    match_dt = parse_kickoff(kickoff_text)
                    match_data["Kickoff"] = match_dt
                    logging.info(f"Kickoff Date and Time: {match_dt}")
                except NoSuchElementException:
                    logging.warning("Kickoff element not found.")
                    match_data["Kickoff"] = "N/A"
                except Exception as e:
                    logging.error(f"Error extracting kickoff information: {e}")
                    match_data["Kickoff"] = "N/A"

                # Extract Team Names
                try:
                    team_elements = match.find_elements(By.CSS_SELECTOR, "div.es-match-teams--item")
                    home_team, away_team = parse_teams(team_elements)
                    match_data["Home"] = home_team
                    match_data["Away"] = away_team
                    logging.info(f"Teams: {home_team} vs {away_team}")
                except Exception as e:
                    logging.error(f"Error extracting team names: {e}")
                    match_data["Home"] = "N/A"
                    match_data["Away"] = "N/A"

                # Extract Odds
                try:
                    # Locate all ds-odd elements within the current match row
                    ds_odds = match.find_elements(By.CSS_SELECTOR, "ds-odd")
                    logging.info(f"Found {len(ds_odds)} ds-odd elements for odds.")
                    odds = parse_odds(driver, ds_odds)

                    # Assign odds to match_data
                    for i in range(1, 10):
                        key = f"Odd {i}"
                        if i <= len(odds):
                            match_data[key] = odds[i-1]
                        else:
                            match_data[key] = "N/A"
                except NoSuchElementException:
                    logging.warning("No ds-odd elements found in the row.")
                except Exception as e:
                    logging.error(f"An error occurred while extracting odds: {e}")

                # Append the extracted data to the list
                all_matches.append(match_data)

                logging.info(f"Scraped Match: {home_team} vs {away_team} at {match_dt} with odds 1: {match_data['Odd 1']}, 2: {match_data['Odd 2']}, 3: {match_data['Odd 3']}, ...")

                if len(all_matches) >= target_match_count:
                    break  # Stop if target is reached

            logging.info(f"\nTotal matches collected: {len(all_matches)}/{target_match_count}")

        # After the scrolling loop, perform a final check to ensure all matches are collected
        if len(all_matches) < target_match_count:
            logging.info("Attempting bidirectional scrolling to load more matches.")
            try:
                # Scroll back to top
                driver.execute_script("arguments[0].scrollTop = 0;", scroll_container)
                logging.info("Scrolled back to the top.")
                time.sleep(2)  # Wait for any dynamic content to load

                # Scroll down again
                driver.execute_script("arguments[0].scrollTop += 500;", scroll_container)
                logging.info("Scrolled down by 500 pixels again.")
                time.sleep(2)

                # Re-fetch rows and process any new matches
                rows = driver.find_elements(By.CSS_SELECTOR, "ion-item-sliding.prematch-top-desk--content")
                logging.info(f"Detected {len(rows)} rows after bidirectional scrolling.")

                for idx, match in enumerate(rows, start=1):
                    if len(all_matches) >= target_match_count:
                        break  # Stop if target is reached

                    # Generate a unique key for the match
                    unique_key = get_unique_match_key(match, idx)
                    if unique_key in processed_matches:
                        continue  # Skip already processed matches

                    # Mark as processed
                    processed_matches.add(unique_key)

                    # Scroll the current match into view
                    try:
                        driver.execute_script("arguments[0].scrollIntoView(true);", match)
                        time.sleep(0.5)  # Small delay to allow any dynamic content to load
                    except Exception as e:
                        logging.error(f"Error scrolling match into view: {e}")

                    logging.info(f"\nProcessing match {len(all_matches)+1}/{target_match_count}")
                    match_data = {
                        "Kickoff": "N/A",
                        "Home": "N/A",
                        "Away": "N/A",
                        "Odd 1": "N/A",
                        "Odd 2": "N/A",
                        "Odd 3": "N/A",
                        "Odd 4": "N/A",
                        "Odd 5": "N/A",
                        "Odd 6": "N/A",
                        "Odd 7": "N/A",
                        "Odd 8": "N/A",
                        "Odd 9": "N/A"
                    }

                    # Extract Kickoff Time and Date
                    try:
                        kickoff_el = match.find_element(By.CSS_SELECTOR, "div.es-match-kickoff")
                        kickoff_text = kickoff_el.text.strip()
                        match_dt = parse_kickoff(kickoff_text)
                        match_data["Kickoff"] = match_dt
                        logging.info(f"Kickoff Date and Time: {match_dt}")
                    except NoSuchElementException:
                        logging.warning("Kickoff element not found.")
                        match_data["Kickoff"] = "N/A"
                    except Exception as e:
                        logging.error(f"Error extracting kickoff information: {e}")
                        match_data["Kickoff"] = "N/A"

                    # Extract Team Names
                    try:
                        team_elements = match.find_elements(By.CSS_SELECTOR, "div.es-match-teams--item")
                        home_team, away_team = parse_teams(team_elements)
                        match_data["Home"] = home_team
                        match_data["Away"] = away_team
                        logging.info(f"Teams: {home_team} vs {away_team}")
                    except Exception as e:
                        logging.error(f"Error extracting team names: {e}")
                        match_data["Home"] = "N/A"
                        match_data["Away"] = "N/A"

                    # Extract Odds
                    try:
                        # Locate all ds-odd elements within the current match row
                        ds_odds = match.find_elements(By.CSS_SELECTOR, "ds-odd")
                        logging.info(f"Found {len(ds_odds)} ds-odd elements for odds.")
                        odds = parse_odds(driver, ds_odds)

                        # Assign odds to match_data
                        for i in range(1, 10):
                            key = f"Odd {i}"
                            if i <= len(odds):
                                match_data[key] = odds[i-1]
                            else:
                                match_data[key] = "N/A"
                    except NoSuchElementException:
                        logging.warning("No ds-odd elements found in the row.")
                    except Exception as e:
                        logging.error(f"An error occurred while extracting odds: {e}")

                    # Append the extracted data to the list
                    all_matches.append(match_data)

                    logging.info(f"Scraped Match: {home_team} vs {away_team} at {match_dt} with odds 1: {match_data['Odd 1']}, 2: {match_data['Odd 2']}, 3: {match_data['Odd 3']}, ...")

                    if len(all_matches) >= target_match_count:
                        break  # Stop if target is reached

                logging.info(f"\nTotal matches collected after bidirectional scrolling: {len(all_matches)}/{target_match_count}")

            except Exception as e:
                logging.error(f"Error during bidirectional scrolling: {e}")

        logging.info(f"\nFinished scrolling. Total matches collected: {len(all_matches)}")

        # Compile all data into a DataFrame
        df = pd.DataFrame(all_matches)

        # Define the mapping from existing column names to desired names
        columns_mapping = {
            "Kickoff": "time",
            "Home": "home",
            "Away": "away",
            "Odd 1": "1",
            "Odd 2": "x",
            "Odd 3": "2",
            "Odd 4": "Over_2.5",
            "Odd 5": "Under_2.5",
            "Odd 6": "Both_Teams_Score",
            "Odd 7": "Home_Win_By_1",
            "Odd 8": "Away_Win_By_1",
            "Odd 9": "Draw_No_Bet"
        }

        # Rename the columns using the mapping
        df.rename(columns=columns_mapping, inplace=True)

        # Display the updated DataFrame
        logging.info("\n--- Extracted Data with Renamed Columns ---")
        logging.info(df)

        # (Optional) Save the extracted data to files
        try:
            # Save to Excel
            df.to_excel("data/takmicenjesoccerbet.xlsx", index=False)
            logging.info("Data saved to 'soccerbetbin.xlsx' with renamed columns.")

            # Save to Pickle
            with open("pickle_data/soccerbetbin.pkl", "wb") as f:
                pickle.dump(df, f)
            logging.info("Data saved to 'soccerbet_matches_debug.pkl' with renamed columns.")
        except Exception as e:
            logging.error(f"Error saving data: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    finally:
        try:
            driver.quit()
            logging.info("WebDriver closed.")
        except Exception as e:
            logging.error(f"Error closing WebDriver: {e}")

if __name__ == "__main__":
    run()
