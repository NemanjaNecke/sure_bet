import logging
import os
import time
import pickle
import pandas as pd
import re
from selenium import webdriver
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
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import datetime

# ---------------------------- Configuration ---------------------------- #

# Configure logging
LOG_DIR = 'log'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'scraper_sportplus.log'),
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------------- Helper Functions ---------------------------- #

def parse_teams(teams_str):
    """
    Parse the teams string to extract home and away team names.
    Expected format:
        'TeamA - TeamB LIVE' --> ('TeamA', 'TeamB')
    """
    try:
        # Remove any trailing text like 'LIVE' if present
        teams_clean = re.sub(r'LIVE', '', teams_str).strip()
        
        # Split by '-' to get home and away teams
        if '-' in teams_clean:
            home, away = teams_clean.split('-', 1)
            home = home.strip()
            away = away.strip()
        else:
            # If format is unexpected, assign 'N/A'
            home, away = "N/A", "N/A"
        
        return home, away
    except Exception as e:
        logging.error(f"Error parsing teams from '{teams_str}': {e}")
        return "N/A", "N/A"

def parse_kickoff(kickoff_text):
    """
    Parse the kickoff text to extract datetime.
    Expected format:
        '26.12.2024(čet.) 18:30' --> datetime object representing the kickoff.
    """
    try:
        # Example input: "26.12.2024(čet.) 18:30"
        # Extract date and time using regex
        match = re.match(r'(\d{2}\.\d{2}\.\d{4})\(\w+\.\)\s+(\d{2}:\d{2})', kickoff_text)
        if match:
            date_part = match.group(1)  # "26.12.2024"
            time_part = match.group(2)  # "18:30"
            # Combine date and time
            date_time_combined = f"{date_part} {time_part}"
            # Parse to datetime object
            match_dt = datetime.datetime.strptime(date_time_combined, "%d.%m.%Y %H:%M")
            return match_dt
        else:
            # If format is unexpected, assign 'N/A'
            return "N/A"
    except Exception as e:
        logging.error(f"Error parsing kickoff information '{kickoff_text}': {e}")
        return "N/A"

def scrape_matches(driver):
    """
    Parses the main match table on the right panel.
    Returns a list of dictionaries, each representing a match.
    """
    matches_data = []
    try:
        # Locate the sport container with specific header
        sport_containers = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, "//div[contains(@class, 'sport soccer') and .//h3[contains(text(), 'NOGOMET')]]")
            )
        )
        for container in sport_containers:
            # Now find the table within sport-body
            table_body = container.find_element(By.CSS_SELECTOR, "div.sport-body table > tbody")
            row_elements = table_body.find_elements(By.TAG_NAME, "tr")
            logging.info(f"Found {len(row_elements)} match rows.")
            for row in row_elements:
                try:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) < 3:
                        logging.warning("Row has fewer than 3 columns, skipping.")
                        continue
                    # Extract text from columns
                    match_no = cols[0].text.strip()
                    teams_str = cols[0].find_element(By.CSS_SELECTOR, "p").text.strip()
                    date_time_str = cols[1].find_element(By.CSS_SELECTOR, "p").text.strip()
                    # The stats column is cols[2], which may contain a link with image, ignored
                    odd_1 = cols[3].find_element(By.TAG_NAME, "button").text.strip()
                    odd_x = cols[4].find_element(By.TAG_NAME, "button").text.strip()
                    odd_2 = cols[5].find_element(By.TAG_NAME, "button").text.strip()
                    odd_1x = cols[6].find_element(By.TAG_NAME, "button").text.strip()
                    odd_x2 = cols[7].find_element(By.TAG_NAME, "button").text.strip()
                    odd_12 = cols[8].find_element(By.TAG_NAME, "button").text.strip()
                    # Count is in the last column, cols[9] if exists
                    count_str = cols[9].find_element(By.CSS_SELECTOR, "p").text.strip() if len(cols) > 9 else "N/A"
                    
                    home_team, away_team = parse_teams(teams_str)
                    match_datetime = parse_kickoff(date_time_str)
                    match_info = {
                        "match_no": match_no,
                        "home": home_team,
                        "away": away_team,
                        "time": match_datetime,
                        "1": odd_1,
                        "x": odd_x,
                        "2": odd_2,
                        "1x": odd_1x,
                        "x2": odd_x2,
                        "12": odd_12,
                        "count": count_str
                    }
                    matches_data.append(match_info)
                except Exception as ex_row:
                    logging.error(f"Error parsing a match row: {ex_row}")
                    continue
    except TimeoutException:
        logging.error("Sport container or matches table not found within the given time.")
    except NoSuchElementException:
        logging.error("No matches table found in the sport container.")
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

def click_on_left_side_percentage(driver, element, label="(unknown)", left_percentage=0.2):
    """
    Clicks on the left side of an element by percentage offset.
    
    Parameters:
    - driver: Selenium WebDriver instance.
    - element: WebElement to interact with.
    - label: Descriptive label for logging.
    - left_percentage: Fraction of the element's width to offset from the left.
    """
    try:
        actions = ActionChains(driver)
        size = element.size
        x_offset = int(size['width'] * left_percentage)
        y_offset = size['height'] // 2
        actions.move_to_element_with_offset(element, x_offset, y_offset).click().perform()
        logging.info(f"Successfully clicked on the left {left_percentage*100}% side of {label} using ActionChains.")
        return True
    except Exception as e:
        logging.error(f"Failed to click on the left {left_percentage*100}% side of {label}: {e}")
        logging.error(f"Failed to click on {label}.")
        return False

def list_all_nogomet_elements(left_menu):
    """
    Lists all 'NOGOMET' menu items within the left menu.
    Returns a list of WebElements.
    """
    try:
        nogomet_elements = left_menu.find_elements(
            By.XPATH, ".//a[contains(@class, 'has-arrow') and contains(@class, 'soccer') and span[normalize-space(text())='NOGOMET']]"
        )
        logging.info(f"Found {len(nogomet_elements)} 'NOGOMET' menu items.")
        return nogomet_elements
    except Exception as e:
        logging.error(f"Error listing 'NOGOMET' menu items: {e}")
        return []

def scroll_offer_scroll(driver):
    """
    Scrolls the 'offerScroll' container to ensure all match rows are loaded.
    """
    try:
        offer_scroll = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "offerScroll"))
        )
        # Scroll to the bottom
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", offer_scroll)
        logging.info("Scrolled 'offerScroll' container to the bottom.")
        time.sleep(1)  # Allow time for dynamic content to load
    except Exception as e:
        logging.error(f"Error scrolling 'offerScroll' container: {e}")

def close_overlays(driver):
    """
    Detects and closes common overlays like cookie consent banners or modals.
    """
    try:
        # Example: Close cookie consent banner
        consent_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'OK') or contains(text(), 'Zatvori')]")
        for button in consent_buttons:
            try:
                button.click()
                logging.info("Closed an overlay by clicking on consent button.")
                time.sleep(1)  # Wait briefly after closing
            except Exception as e:
                logging.warning(f"Failed to click on consent button: {e}")
    except Exception as e:
        logging.error(f"Error while trying to close overlays: {e}")

def click_svi_dani(driver):
    """
    Clicks the 'Svi dani' (All Days) button to load matches for all days.
    """
    try:
        # Locate the 'Svi dani' button by its text
        svi_dani_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[normalize-space(text())='Svi dani']")
            )
        )
        logging.info("Located 'Svi dani' button.")
        
        # Click the 'Svi dani' button
        if safe_click(driver, svi_dani_button, label="Svi dani Button"):
            logging.info("Clicked on 'Svi dani' button successfully.")
            # Wait for matches to load after clicking 'Svi dani'
            time.sleep(2)  # Adjust as necessary based on page behavior
        else:
            logging.error("Failed to click on 'Svi dani' button.")
    except TimeoutException:
        logging.error("'Svi dani' button not found within the given time.")
    except Exception as e:
        logging.error(f"Error clicking on 'Svi dani' button: {e}")

def click_ostalo(driver):
    """
    Clicks the 'Ostalo' button to load matches for all days.
    """
    try:
        # Locate the 'Svi dani' button by its text
        svi_dani_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[normalize-space(text())='Ostalo']")
            )
        )
        logging.info("Located 'Ostalo' button.")
        
        # Click the 'Svi dani' button
        if safe_click(driver, svi_dani_button, label="Ostalo Button"):
            logging.info("Clicked on 'Ostalo' button successfully.")
            # Wait for matches to load after clicking 'Svi dani'
            time.sleep(2)  # Adjust as necessary based on page behavior
        else:
            logging.error("Failed to click on 'Ostalo' button.")
    except TimeoutException:
        logging.error("'Ostalo' button not found within the given time.")
    except Exception as e:
        logging.error(f"Error clicking on 'Ostalo' button: {e}")

# ---------------------------- Main Scraper Function ---------------------------- #

def run():
    """
    Main function to execute the scraping process.
    """
    url = "https://www.sportplus.ba/prematch/betting"
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
        close_overlays(driver)  # Close any pop-ups or overlays
        # Wait until the left menu is present
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "left-menu"))
        )
        logging.info("Page loaded and left menu is present.")
    except Exception as e:
        logging.error(f"Error navigating to {url}: {e}")
        driver.quit()
        return

    all_matches = []

    # 1) Locate the "NOGOMET" (Football) section in the left menu
    try:
        left_menu = driver.find_element(By.ID, "left-menu")
        logging.info("Located the left menu.")
    except NoSuchElementException:
        logging.error("Left menu not found. Exiting.")
        driver.quit()
        return

    # List all 'NOGOMET' elements to ensure targeting the correct one
    click_ostalo(driver)
    nogomet_elements = list_all_nogomet_elements(left_menu)
    if not nogomet_elements:
        logging.error("No 'NOGOMET' menu items found. Exiting.")
        driver.quit()
        return

    # Handle multiple 'NOGOMET' menu items if present
    for index, nogomet_menu in enumerate(nogomet_elements):
        logging.info(f"Attempting to click on 'NOGOMET' menu item {index + 1}.")
        # Adjust the left_percentage as needed to ensure accurate clicking
        if click_on_left_side_percentage(driver, nogomet_menu, label=f"NOGOMET Menu {index + 1}", left_percentage=0.48):
            logging.info(f"Clicked on 'NOGOMET' menu item {index + 1} successfully.")
            break
        else:
            logging.warning(f"Failed to click on 'NOGOMET' menu item {index + 1}.")
    else:
        logging.error("Failed to click on any 'NOGOMET' menu items. Exiting.")
        driver.quit()
        return

    logging.info("Clicked on 'NOGOMET' menu to load matches.")

    time.sleep(0.5)  # Adjust sleep time as necessary

    # 4) Click on each submenu under 'NOGOMET' to load matches
    try:
        # Locate the expanded submenu under 'NOGOMET'
        submenu_container = left_menu.find_element(
            By.XPATH, ".//a[contains(@class, 'has-arrow') and contains(@class, 'soccer') and span[normalize-space(text())='NOGOMET']]/following-sibling::ul"
        )
        submenu_items = submenu_container.find_elements(By.XPATH, ".//li/a/span")
        logging.info(f"Found {len(submenu_items)} submenu items under 'NOGOMET'.")
        
        for i, submenu in enumerate(submenu_items, start=0):
            try:
                if i > 1:
                    # safe_click(driver, submenu, label=f"Submenu {submenu.text}")
                    # ActionChains.double_click(submenu)
                    click_svi_dani(driver)
                    break
                # Click on each submenu to load its matches
                safe_click(driver, submenu, label=f"Submenu {submenu.text}")
                logging.info(f"Clicked on Submenu {submenu.text} successfully.")
                time.sleep(1)  # Wait for matches to load
                # Optionally, you can scrape matches after each submenu click
                # Uncomment the following lines if you prefer to scrape incrementally
                # matches = scrape_matches(driver)
                # logging.info(f"Scraped {len(matches)} matches from Submenu {i}.")
                # all_matches.extend(matches)
            except Exception as e:
                logging.error(f"Error clicking on Submenu {submenu.text}: {e}")
                continue
    except NoSuchElementException:
        logging.error("Submenu container under 'NOGOMET' not found. Exiting.")
        driver.quit()
        return
    except Exception as e:
        logging.error(f"Error locating submenu items: {e}")
        driver.quit()
        return

    scroll_attempts = 0
    max_scroll_attempts = 1
    last_height = driver.execute_script(
            "return arguments[0].scrollHeight;", WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "offerScroll"))
        )
        )
    while scroll_attempts < max_scroll_attempts:
    # 6) Scroll  if matches load dynamically upon clicking 'Svi dani'
        scroll_offer_scroll(driver)
        new_height = driver.execute_script("return arguments[0].scrollHeight;", WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "offerScroll"))))
        # 7) Scrape the matches from the loaded page
        if new_height == last_height:
            scroll_attempts += 1
            logging.info(f"No new content. scroll_attempts={scroll_attempts}/{max_scroll_attempts}")
            if scroll_attempts >= max_scroll_attempts:
                logging.info("Max scroll attempts reached. Stopping scroll.")
                break
        else:
            last_height = new_height
            scroll_attempts = 0
      # Adjust sleep time as necessary
    try:
        matches = scrape_matches(driver)
        logging.info(f"Scraped {len(matches)} matches.")
        all_matches.extend(matches)
    except Exception as e:
        logging.error(f"Error during match scraping: {e}")

    # 8) Close the WebDriver
    try:
        driver.quit()
        logging.info("WebDriver closed successfully.")
    except Exception as e:
        logging.error(f"Error closing WebDriver: {e}")

    # 9) Save the scraped data
    if all_matches:
        try:
            df = pd.DataFrame(all_matches)
            logging.info(f"DataFrame created with {len(df)} rows.")

            # Save to Excel
            os.makedirs("data", exist_ok=True)
            excel_path = "data/takmicenjesportplus.xlsx"
            df.to_excel(excel_path, index=False)
            logging.info(f"Data saved to {excel_path}")

            # Save to Pickle
            os.makedirs("pickle_data", exist_ok=True)
            pickle_path = "pickle_data/takmicenjesportplusbin.pkl"
            with open(pickle_path, "wb") as f:
                pickle.dump(df, f)
            logging.info(f"Data pickled to {pickle_path}")
        except Exception as e:
            logging.error(f"Error saving data: {e}")
    else:
        logging.info("No matches were scraped.")

if __name__ == "__main__":
    run()
