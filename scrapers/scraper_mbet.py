import os
import time
import pickle
import logging
import datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
    ElementClickInterceptedException
)
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

def safe_click(driver, element, label="(unknown)"):
    """
    Attempts to click an element:
     1) Regular .click()
     2) JS click if intercepted
    Logs a warning if both fail.
    """
    try:
        element.click()
        return True
    except ElementClickInterceptedException as e1:
        logging.warning(f"ElementClickIntercepted on {label}: {e1}. Trying JS click.")
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as e2:
            logging.warning(f"JS click also failed for {label}: {e2}")
            return False
    except Exception as e:
        logging.warning(f"Unexpected click failure on {label}: {e}")
        return False

def run():
    log_dir = "log"
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_dir, "scraper_mbet.log"),
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Starting scraper_mbet")
    web = "https://mbet.ba/prematch"

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--headless")  # Uncomment if needed
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # Prepare lists for final DataFrame
    match_times   = []
    match_dates   = []
    home_teams    = []
    away_teams    = []
    odds_1        = []
    odds_x        = []
    odds_2        = []

    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        logging.info("WebDriver initialized successfully.")
    except WebDriverException as e:
        logging.error(f"Error initializing WebDriver: {e}")
        return

    try:
        driver.get(web)
        logging.info(f"Navigated to {web}")
        time.sleep(2)

        current_year = datetime.datetime.now().year

        # Locate the Fudbal container
        try:
            fudbal_container = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "//div[contains(@class, 'global-style_nav_item__') "
                    "      and .//div[contains(@class, 'global-style_nav_item_label__') and text()='Fudbal']]"
                ))
            )
            logging.info("Located the Fudbal container.")
        except TimeoutException:
            logging.error("Fudbal container not found. Exiting.")
            driver.quit()
            return

        # Find the subnav container inside that Fudbal container
        try:
            subnav_container = fudbal_container.find_element(
                By.CSS_SELECTOR, "div[class*='global-style_subnav__']"
            )
            logging.info("Found subnav container inside the Fudbal container.")
        except NoSuchElementException:
            logging.error("No subnav container in the Fudbal container. Exiting.")
            driver.quit()
            return

        # Collect subcategory blocks
        subcategories = subnav_container.find_elements(
            By.XPATH, ".//div[contains(@class, 'global-style_nav_item_content__')]"
        )
        logging.info(f"Found {len(subcategories)} subcategories in Fudbal container.")

        subcat_labels = []
        for idx, subcat in enumerate(subcategories):
            try:
                label_el = subcat.find_element(
                    By.CSS_SELECTOR, "div[class*='global-style_nav_item_label__']"
                )
                label_text = label_el.get_attribute("textContent").strip()
                if not label_text:
                    logging.info(f"Skipping subcat at idx={idx}, label is empty.")
                    continue
                if "BONUS TIP" in label_text.upper():
                    logging.info(f"Skipping bonus subcat: {label_text}")
                    continue

                subcat_labels.append(label_text)
                logging.info(f"Added subcat label: {label_text}")
            except Exception as sub_e:
                logging.warning(f"Problem reading label from subcat idx={idx}: {sub_e}")

        logging.info(f"Total subcategories: {len(subcat_labels)} => {subcat_labels}")

        # Now iterate over each subcat label
        for label in subcat_labels:
            try:
                logging.info(f"Processing subcategory: {label}")
                if "ENGLAND 1 (England)" in label or "Košarka special" in label:
                    logging.info(f"Skipping click for already present subcategory: {label}")
                # Re-find the subcategory element each loop
                else:
                    subcat_xpath = (
                        f".//div[contains(@class, 'global-style_nav_item_label__') "
                        f" and normalize-space(text())='{label}']"
                        "/ancestor::div[contains(@class, 'global-style_nav_item_content__')]"
                    )

                    subcat_elem = WebDriverWait(subnav_container, 10).until(
                        EC.presence_of_element_located((By.XPATH, subcat_xpath))
                    )

                    # Scroll subcat element into center
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", subcat_elem)
                    time.sleep(0.5)

                    # Optional: Hover using ActionChains
                    ActionChains(driver).move_to_element(subcat_elem).pause(0.3).perform()

                    # Attempt to click
                    if not safe_click(driver, subcat_elem, label=label):
                        logging.warning(f"Skipping subcategory '{label}' - could not click.")
                        continue

                    time.sleep(2)  # let matches load

                # Scroll multiple times
                for _ in range(3):
                    driver.execute_script("window.scrollBy(0, 1200);")
                    time.sleep(1)

                # Now collect match rows
                match_rows = driver.find_elements(By.CSS_SELECTOR, "div[class*='global-style_table_row__']")
                logging.info(f"Found {len(match_rows)} matches under subcategory: {label}")

                # Parse each row
                for row_el in match_rows:
                    try:
                        # Time & date
                        time_col = row_el.find_element(
                            By.CSS_SELECTOR, "div[class*='global-style_time_col__']"
                        )
                        t_el = time_col.find_element(By.CSS_SELECTOR, "span[class*='global-style_time__']")
                        d_el = time_col.find_element(By.CSS_SELECTOR, "span[class*='global-style_date__']")

                        t_text = t_el.get_attribute("textContent").strip()
                        d_text = d_el.get_attribute("textContent").strip()

                        # Teams
                        home_text = away_text = ""
                        try:
                            home_el = row_el.find_element(
                                By.CSS_SELECTOR, "div[class*='global-style_team_home__']"
                            )
                            away_el = row_el.find_element(
                                By.CSS_SELECTOR, "div[class*='global-style_team_away__']"
                            )
                            home_text = home_el.get_attribute("textContent").strip()
                            away_text = away_el.get_attribute("textContent").strip()
                        except Exception as e_team:
                            logging.debug(f"No home/away found for row: {e_team}")

                        # Try parse
                        dt_combined = None
                        try:
                            dt_combined = datetime.datetime.strptime(
                                f"{d_text} {current_year} {t_text}",
                                "%d.%m. %Y %H:%M"
                            )
                        except ValueError as ve:
                            logging.warning(f"Date parse error for '{home_text} vs {away_text}': {ve}")

                        # Odds (1, x, 2)
                        odds_block = row_el.find_elements(
                            By.CSS_SELECTOR, "div[class*='global-style_market_width_3__']"
                        )
                        o1, ox, o2 = "", "", ""
                        if odds_block:
                            first_market = odds_block[0]
                            stake_btns = first_market.find_elements(
                                By.CSS_SELECTOR,
                                "button[class*='global-style_stake_type_btn__']"
                            )
                            if len(stake_btns) >= 3:
                                o1 = stake_btns[0].get_attribute("textContent").strip()
                                ox = stake_btns[1].get_attribute("textContent").strip()
                                o2 = stake_btns[2].get_attribute("textContent").strip()

                        # Only append if we have a valid parsed date/time
                        if dt_combined:
                            match_times.append(dt_combined)
                            match_dates.append(d_text)
                            home_teams.append(home_text)
                            away_teams.append(away_text)
                            odds_1.append(o1)
                            odds_x.append(ox)
                            odds_2.append(o2)
                        else:
                            logging.warning(f"Skipping row: no valid date/time for '{home_text} vs {away_text}'.")

                    except Exception as row_ex:
                        logging.warning(f"Error parsing match row in subcat '{label}': {row_ex}")

                logging.info(f"Done subcategory: {label}")

            except Exception as cat_ex:
                logging.error(f"Error with subcategory '{label}': {cat_ex}")

        # Build final DataFrame
        data_dict = {
            "time": match_times,
            "date": match_dates,
            "home": home_teams,
            "away": away_teams,
            "1": odds_1,
            "x": odds_x,
            "2": odds_2
        }
        df = pd.DataFrame(data_dict)
        df.drop_duplicates(inplace=True)
        logging.info(f"\nTotal matches collected: {len(df)}")

        # Output to logs
        logging.info("\n--- Extracted Data ---")
        if not df.empty:
            logging.info(df.head(10).to_string())
        else:
            logging.info("No data extracted.")

        # Save to Excel / Pickle
        try:
            os.makedirs("data", exist_ok=True)
            df.to_excel("data/takmicenjembet.xlsx", index=False)
            logging.info("Data saved to 'takmicenjembet.xlsx'")

            os.makedirs("pickle_data", exist_ok=True)
            with open("pickle_data/mbetbin.pkl", "wb") as f:
                pickle.dump(df, f)
            logging.info("Data saved to 'pickle_data/mbetbin.pkl'")
        except Exception as save_ex:
            logging.error(f"Error saving data: {save_ex}")

    except Exception as main_ex:
        logging.error(f"Unexpected error in main logic: {main_ex}")

    finally:
        try:
            driver.quit()
            logging.info("Driver closed.")
        except Exception as close_ex:
            logging.error(f"Error closing WebDriver: {close_ex}")

if __name__ == "__main__":
    run()
