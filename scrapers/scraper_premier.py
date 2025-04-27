import logging
import os
import time
import pickle
import pandas as pd
import re
import datetime

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    WebDriverException
)

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# Configure logging
logging.basicConfig(
    filename='log/scraperpremier.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def parse_teams(teams_str: str) -> (str, str):
    """
    Parse the teams string (e.g. "Pisa-SassuoloLIVETV") into home and away team names.
    """
    try:
        teams_clean = re.sub(r'LIVETV', '', teams_str).strip()
        if '-' in teams_clean:
            home, away = teams_clean.split('-', 1)
            return home.strip(), away.strip()
        else:
            return "N/A", "N/A"
    except Exception as e:
        logging.error(f"Error parsing teams from '{teams_str}': {e}")
        return "N/A", "N/A"

def parse_kickoff(kickoff_text: str):
    """
    Parse a kickoff text like "26.12. čet,12:30" into a datetime.
    Returns datetime or "N/A" on error.
    """
    try:
        date_part, time_part = kickoff_text.split(',', 1)
        date_clean = re.sub(r'\s+\w+', '', date_part).strip()
        current_year = datetime.datetime.now().year
        date_time_str = f"{date_clean} {time_part.strip()} {current_year}"
        return datetime.datetime.strptime(date_time_str, "%d.%m. %H:%M %Y")
    except Exception as e:
        logging.error(f"Error parsing kickoff '{kickoff_text}': {e}")
        return "N/A"

def hover_element(actions, element, label="(unknown)") -> bool:
    """
    Safely hovers over an element to reveal submenus.
    """
    try:
        actions.move_to_element(element).perform()
        time.sleep(1.0)  # allow sub-menu to appear
        return True
    except StaleElementReferenceException as e:
        logging.warning(f"Stale while hovering over {label}: {e}")
        return False

def click_element(element, description: str, retries: int = 3) -> bool:
    """
    Tries to click an element with a few retries for stale or intercept errors.
    """
    for attempt in range(retries):
        try:
            element.click()
            logging.info(f"Successfully clicked {description}.")
            return True
        except (ElementClickInterceptedException, StaleElementReferenceException) as e:
            logging.warning(f"[Attempt {attempt+1}] Error clicking {description}: {e}")
            time.sleep(1)
    logging.error(f"Failed to click {description} after {retries} attempts.")
    return False

def scrape_all_tables(driver) -> list:
    """
    Scrapes *all* tables within the right panel:
      <div class="prPrikaz-overflow-container">
         <table class="ponTablica"> ... </table>
         <table class="ponTablica"> ... </table>
         ...
    Returns a list of unique match dictionaries (deduplicated).
    """
    all_matches = {}
    try:
        # Wait until at least one table is visible
        WebDriverWait(driver, 15).until(
            EC.visibility_of_element_located((
                By.CSS_SELECTOR,
                "div.prPrikaz-overflow-container table.ponTablica"
            ))
        )

        # Collect all table bodies
        table_bodies = driver.find_elements(
            By.CSS_SELECTOR,
            "div.prPrikaz-overflow-container table.ponTablica > tbody"
        )
        logging.info(f"Found {len(table_bodies)} <tbody> elements for ponTablica tables.")

        for t_idx, table_body in enumerate(table_bodies, start=1):
            try:
                rows = table_body.find_elements(By.CSS_SELECTOR, "tr[data-who]")
                logging.info(f"Table {t_idx}: Found {len(rows)} match rows.")
                for row_idx, row in enumerate(rows, start=1):
                    try:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if len(cols) < 9:
                            logging.warning(f"Table {t_idx} Row {row_idx} has <9 columns. Skipping.")
                            continue
                        match_no    = cols[0].text.strip()
                        teams_str   = cols[1].text.strip()
                        date_time_str = cols[2].text.strip()
                        odd_1       = cols[3].text.strip()
                        odd_x       = cols[4].text.strip()
                        odd_2       = cols[5].text.strip()
                        odd_1x      = cols[6].text.strip()
                        odd_x2      = cols[7].text.strip()
                        odd_12      = cols[8].text.strip()

                        home, away  = parse_teams(teams_str)
                        kickoff_dt  = parse_kickoff(date_time_str)

                        # Use a tuple (match_no, home, away) as unique key
                        key = (match_no, home, away)
                        if key not in all_matches:
                            all_matches[key] = {
                                "match_no": match_no,
                                "home": home,
                                "away": away,
                                "time": kickoff_dt,
                                "1":  odd_1,
                                "x":  odd_x,
                                "2":  odd_2,
                                "1x": odd_1x,
                                "x2": odd_x2,
                                "12": odd_12
                            }
                    except Exception as row_ex:
                        logging.error(f"Error parsing row in Table {t_idx}, row {row_idx}: {row_ex}")
            except Exception as tb_ex:
                logging.error(f"Error parsing Table {t_idx}: {tb_ex}")
    except TimeoutException:
        logging.info("No tables loaded within 15s. Possibly no matches found or slow page.")
    except NoSuchElementException:
        logging.info("No main table found in this view.")
    except Exception as e:
        logging.error(f"General error scraping all tables: {e}")

    return list(all_matches.values())

def run():
    url = "https://www.premier-kladionica.com/ponuda"
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--headless")

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = None

    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        logging.info("WebDriver initialized successfully.")

        driver.get(url)
        logging.info(f"Navigated to {url}")
        time.sleep(3)  # let main page load

        actions = ActionChains(driver)
        all_matches = []

        # 1) Locate "SVE" button
        try:
            sve_button = WebDriverWait(driver, 40).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//span[@data-what='toggle' and contains(text(), 'SVE')]")
                )
            )
            logging.info(f"Located 'SVE' button: {sve_button.text}")
        except TimeoutException:
            logging.error("Could not locate 'SVE' button. Exiting.")
            return

        # 2) Hover over "SVE"
        if not hover_element(actions, sve_button, label="'SVE' button"):
            logging.error("Failed to hover over 'SVE' button. Exiting.")
            return
        logging.info("Hovered on 'SVE' button to reveal submenu.")

        # 3) Wait for the parent li to have 'subm' class
        try:
            sve_li = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//span[@data-what='toggle' and contains(text(), 'SVE')]"
                        "/ancestor::li[contains(@class, 'subm')]"
                    )
                )
            )
            logging.info("'SVE' parent li with subm/doScroll/noScroll is visible.")
        except TimeoutException as e:
            logging.error(f"Submenu not visible after hovering over 'SVE': {e}")
            return

        # 4) Locate "NOGOMET" toggler
        try:
            nogomet_toggle = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//li[contains(@class, 'subm')]"
                        "//span[@data-what='toggle' and contains(text(), 'NOGOMET')]"
                    )
                )
            )
            logging.info(f"Located NOGOMET toggle: {nogomet_toggle.text}")
        except TimeoutException:
            logging.error("Could not locate 'NOGOMET' toggler. Exiting.")
            return

        # 5) Find the addRm next to "NOGOMET" toggle & click
        try:
            nogomet_addrm = nogomet_toggle.find_element(
                By.XPATH,
                "./following-sibling::span[@data-what='addRm']"
            )
            if not click_element(nogomet_addrm, "addRm for 'NOGOMET'"):
                logging.error("Could not click addRm for 'NOGOMET'. Exiting.")
                return
            logging.info("Clicked addRm for 'NOGOMET'. Waiting for matches to load...")

            # Optional unhover: move mouse away from the left panel to close the sub-menu
            actions.move_by_offset(300, 0).perform()
            time.sleep(6)  # Wait longer for large match-lists to load fully

            # Now do a *single pass* scraping:
            nogo_matches = scrape_all_tables(driver)
            logging.info(f"Scraped {len(nogo_matches)} 'NOGOMET' matches.")
            all_matches.extend(nogo_matches)

        except NoSuchElementException as e:
            logging.error(f"Could not find addRm near 'NOGOMET' toggler: {e}")
            return

        logging.info(f"Total matches collected: {len(all_matches)}")

        # 6) Save results
        df = pd.DataFrame(all_matches)
        logging.info(f"DataFrame created with {len(df)} rows.")

        os.makedirs("data", exist_ok=True)
        df.to_excel("data/takmicenjepremier.xlsx", index=False)
        logging.info("Data saved to data/takmicenjepremier.xlsx")

        os.makedirs("pickle_data", exist_ok=True)
        with open("pickle_data/takmicenjepremierbin.pkl", "wb") as f:
            pickle.dump(df, f)
        logging.info("Data pickled to pickle_data/takmicenjepremierbin.pkl")

    except Exception as e:
        logging.error(f"Unhandled exception in run(): {e}")

    finally:
        if driver:
            driver.quit()
            logging.info("WebDriver closed.")

def main():
    try:
        run()
    except Exception as ex:
        logging.error(f"Unhandled exception in main(): {ex}")

if __name__ == "__main__":
    main()
