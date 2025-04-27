import logging
import os
import time
import pickle
import pandas as pd
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
    ElementClickInterceptedException,
    ElementNotInteractableException
)
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import datetime

# ---------------------------- Configuration ---------------------------- #

# Configure logging
LOG_DIR = 'log'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'scraper_meridianbet.log'),
    filemode='w',
    level=logging.INFO,  # Adjust to WARNING or ERROR to reduce verbosity
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------------- Helper Functions ---------------------------- #

def map_odds_title(title, home_team, away_team):
    """
    Maps the title attribute to standardized keys.
    - Home win: '1'
    - Draw: 'X'
    - Away win: '2'
    - Other bet types are returned as is.
    """
    if home_team in title and "pobeđuje na meču" in title:
        return '1'
    elif "Nerešen ishod na meču" in title:
        return 'x'
    elif away_team in title and "pobeđuje na meču" in title:
        return '2'
    else:
        # For other bet types, extract a standardized name
        # Example: "Oba tima postižu bar po 1 gol na meču" -> "Both Teams to Score"
        # You can customize this mapping as needed
        # For simplicity, we'll return a cleaned version of the title
        standardized_title = title.replace("pobeđuje na meču", "").strip()
        return standardized_title

def parse_teams(home_str, away_str):
    """
    Extract home and away team names.
    """
    try:
        home = home_str.strip()
        away = away_str.strip()
        return home, away
    except Exception as e:
        logging.error(f"Error parsing teams: {e}")
        return "N/A", "N/A"

def parse_kickoff(time_str, date_str):
    """
    Combine time and date strings to create a datetime object.
    Expected formats:
        time_str: "15:30"
        date_str: "29.12"
    Assumes the current year if the year is not provided.
    """
    try:
        # Check if date_str includes year
        if len(date_str.split('.')) == 2:
            # Append current year
            current_year = datetime.datetime.now().year
            date_str += f".{current_year}"
        # Combine date and time
        date_time_combined = f"{date_str} {time_str}"
        # Parse to datetime object
        match_dt = datetime.datetime.strptime(date_time_combined, "%d.%m.%Y %H:%M")
        return match_dt
    except Exception as e:
        logging.error(f"Error parsing kickoff information '{time_str} {date_str}': {e}")
        return "N/A"

def scrape_matches(driver):
    """
    Scrapes the match data from the page using updated CSS selectors.
    Returns a list of dictionaries, each representing a match.
    """
    matches_data = []
    try:
        # Corrected Selector: Locate all divs with class 'c-event' inside 'standard-event' components
        match_elements = WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "standard-event div.c-event")
            )
        )
        logging.info(f"Found {len(match_elements)} match elements.")

        for match in match_elements:
            try:
                # Extract kickoff time and date
                time_element = match.find_element(By.CSS_SELECTOR, "div.c-event__period-time")
                date_element = match.find_element(By.CSS_SELECTOR, "div.c-event__period-min")
                time_str = time_element.text.strip()
                date_str = date_element.text.strip()

                # Extract home and away team names
                home_element = match.find_element(By.CSS_SELECTOR, "div.c-event__rivals--home > span")
                away_element = match.find_element(By.CSS_SELECTOR, "div.c-event__rivals--away > span")
                home_str = home_element.text.strip()
                away_str = away_element.text.strip()

                # Parse teams and kickoff
                home_team, away_team = parse_teams(home_str, away_str)
                match_datetime = parse_kickoff(time_str, date_str)

                # Extract odds
                odds_elements = match.find_elements(By.CSS_SELECTOR, "div.c-selection")
                odds = {}
                for odd in odds_elements:
                    title = odd.get_attribute('title').strip()
                    value = odd.text.strip()
                    mapped_key = map_odds_title(title, home_team, away_team)
                    odds[mapped_key] = value

                match_info = {
                    "home": home_team,
                    "away": away_team,
                    "time": match_datetime,
                }

                # Add odds to match_info
                for key, val in odds.items():
                    match_info[key] = val

                matches_data.append(match_info)
            except NoSuchElementException as ex_row:
                logging.error(f"NoSuchElementException parsing a match: {ex_row}")
                continue
            except Exception as ex_row:
                logging.error(f"Error parsing a match: {ex_row}")
                continue
    except TimeoutException:
        logging.error("Match elements not found within the given time.")
    except NoSuchElementException:
        logging.error("No matches found on the page.")
    except Exception as e:
        logging.error(f"Unexpected error in scrape_matches: {e}")
    return matches_data

def safe_click(driver, element, label="(unknown)"):
    """
    Attempts to click an element:
     1) Regular .click()
     2) ActionChains click
     3) JS click if intercepted or not interactable
    Logs a warning if all methods fail.
    """
    try:
        # Scroll the element into view
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        # Wait briefly to ensure it's scrolled
        time.sleep(0.5)

        # Attempt regular click
        element.click()
        logging.info(f"Successfully clicked on {label} using regular click.")
        return True
    except (ElementClickInterceptedException, ElementNotInteractableException) as e:
        logging.warning(f"{e.__class__.__name__} on {label}: {e}. Trying ActionChains click.")
        try:
            actions = ActionChains(driver)
            actions.move_to_element(element).click().perform()
            logging.info(f"Successfully clicked on {label} using ActionChains.")
            return True
        except Exception as e_act:
            logging.warning(f"ActionChains click failed for {label}: {e_act}. Trying JS click.")
            try:
                driver.execute_script("arguments[0].click();", element)
                logging.info(f"Successfully clicked on {label} using JS click.")
                return True
            except Exception as e_js:
                logging.warning(f"JS click also failed for {label}: {e_js}")
                logging.error(f"Failed to click on {label}.")
                return False
    except Exception as e:
        logging.warning(f"Unexpected exception when clicking on {label}: {e}. Trying JS click.")
        try:
            driver.execute_script("arguments[0].click();", element)
            logging.info(f"Successfully clicked on {label} using JS click.")
            return True
        except Exception as e_js:
            logging.warning(f"JS click failed for {label}: {e_js}")
            logging.error(f"Failed to click on {label}.")
            return False

def close_initial_overlay(driver):
    """
    Closes the initial overlay/pop-up on the page.
    """
    try:
        # Locate the overlay close button by its data attributes
        overlay = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "div.element[data-id='Element99'][data-element='close-1']")
            )
        )
        if safe_click(driver, overlay, label="Initial Overlay Close Button"):
            logging.info("Closed the initial overlay successfully.")
        else:
            logging.error("Failed to close the initial overlay.")
    except TimeoutException:
        logging.warning("Initial overlay not found; it might have already been closed.")
    except Exception as e:
        logging.error(f"Error while attempting to close the initial overlay: {e}")

def click_sve_button(driver):
    """
    Clicks the 'Sve' (All) button to load all available matches.
    """
    try:
        # Locate the 'Sve' button by its text within the event filters
        sve_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[contains(@class, 'c-event-filter__tab') and normalize-space(text())='Sve']")
            )
        )
        if safe_click(driver, sve_button, label="'Sve' Button"):
            logging.info("Clicked on 'Sve' button successfully.")
            time.sleep(2)  # Wait for the content to load after clicking

            # Additional verification: Check if matches are loaded
            try:
                # Attempt to locate a known match element or a container that should appear after clicking 'Sve'
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "standard-event div.c-event")
                    )
                )
                logging.info("'Sve' button click resulted in matches being loaded.")
            except TimeoutException:
                logging.error("After clicking 'Sve', match elements were not loaded.")
        else:
            logging.error("Failed to click on 'Sve' button.")
    except TimeoutException:
        logging.error("'Sve' button not found within the given time.")
    except Exception as e:
        logging.error(f"Error clicking on 'Sve' button: {e}")

def scroll_to_load_all(driver):
    """
    Scrolls the correct container to ensure all dynamic content is loaded.
    Implements smooth scrolling in smaller increments.
    """
    try:
        # Identify the correct scroller based on the provided HTML
        scroller = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.l-betting-page")
            )
        )
        last_height = driver.execute_script("return arguments[0].scrollHeight", scroller)
        current_position = 0
        scroll_increment = 300  # Pixels to scroll each step
        pause_time = 0.3  # Pause between scrolls

        while current_position < last_height:
            # Scroll down by the increment
            driver.execute_script(
                "arguments[0].scrollTo(arguments[0].scrollTop + arguments[1], arguments[0].scrollTop + arguments[1]);",
                scroller,
                scroll_increment
            )
            current_position += scroll_increment
            logging.info(f"Scrolled to position {current_position} / {last_height}")
            time.sleep(pause_time)
            # Update scroll_height in case new content was loaded
            new_scroll_height = driver.execute_script("return arguments[0].scrollHeight", scroller)
            if new_scroll_height > last_height:
                last_height = new_scroll_height

        logging.info("Completed smooth scrolling.")
    except TimeoutException:
        logging.error("Scroller container not found within the given time.")
    except Exception as e:
        logging.error(f"Error during scrolling: {e}")

def close_overlays(driver):
    """
    Detects and closes additional overlays like cookie consent banners or modals.
    """
    try:
        # Example: Close cookie consent banner
        consent_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Prihvatam') or contains(text(), 'Zatvori')]")
        for button in consent_buttons:
            try:
                button.click()
                logging.info("Closed an overlay by clicking on consent button.")
                time.sleep(1)  # Wait briefly after closing
            except Exception as e:
                logging.warning(f"Failed to click on consent button: {e}")
    except Exception as e:
        logging.error(f"Error while trying to close overlays: {e}")

# ---------------------------- Main Scraper Function ---------------------------- #

def run():
    """
    Main function to execute the scraping process.
    """
    url = "https://meridianbet.ba/sr/kladjenje/fudbal"
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--headless")  # Run in headless mode; comment out for debugging
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    # Optionally, set a custom User-Agent to mimic real browser behavior
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )

    # Initialize WebDriver
    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), 
            options=options
        )
        logging.info("WebDriver initialized successfully.")
    except WebDriverException as e:
        logging.error(f"Error initializing WebDriver: {e}")
        return

    # Navigate to the target URL
    try:
        driver.get(url)
        logging.info(f"Navigated to {url}")
        close_overlays(driver)  # Close any additional pop-ups or overlays
        close_initial_overlay(driver)  # Close the specific initial overlay
        logging.info("Page loaded and initial overlays are closed.")
    except Exception as e:
        logging.error(f"Error navigating to {url}: {e}")
        driver.quit()
        return

    all_matches = []

    # 1) Click on the 'Sve' button to load all matches
    click_sve_button(driver)

    # 2) Scroll to load all dynamic content
    scroll_to_load_all(driver)

    # 3) Scrape the match data
    try:
        matches = scrape_matches(driver)
        logging.info(f"Scraped a total of {len(matches)} matches.")
        all_matches.extend(matches)
    except Exception as e:
        logging.error(f"Error during match scraping: {e}")

    # 4) Close the WebDriver
    try:
        driver.quit()
        logging.info("WebDriver closed successfully.")
    except Exception as e:
        logging.error(f"Error closing WebDriver: {e}")

    # 5) Save the scraped data
    if all_matches:
        try:
            # Identify all unique columns excluding 'home', 'away', and 'kickoff'
            additional_columns = set()
            for match in all_matches:
                for key in match.keys():
                    if key not in ['home', 'away', 'kickoff']:
                        additional_columns.add(key)
            # Define the order of columns
            columns = ['home', 'away'] + sorted(additional_columns)

            # Create DataFrame
            df = pd.DataFrame(all_matches, columns=columns)
            
            logging.info(f"DataFrame created with {len(df)} rows and columns: {df.columns.tolist()}.")

            # Save to Excel
            os.makedirs("data", exist_ok=True)
            excel_path = "data/meridianbet_fudbal.xlsx"
            df.to_excel(excel_path, index=False)
            logging.info(f"Data saved to {excel_path}")

            # Save to Pickle
            os.makedirs("pickle_data", exist_ok=True)
            pickle_path = "pickle_data/meridianbet_fudbal.pkl"
            with open(pickle_path, "wb") as f:
                pickle.dump(df, f)
            logging.info(f"Data pickled to {pickle_path}")
        except Exception as e:
            logging.error(f"Error saving data: {e}")
    else:
        logging.info("No matches were scraped.")

if __name__ == "__main__":
    run()
