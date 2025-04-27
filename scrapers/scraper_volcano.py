import logging
import os
import time
import pickle
import pandas as pd
import datetime
import warnings

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# --------------------- #
# 1. Suppress Warnings  #
# --------------------- #

# Suppress pandas SettingWithCopyWarning
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

# Suppress TensorFlow warnings if applicable
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # FATAL only

# --------------------- #
# 2. Helper Functions   #
# --------------------- #

def parse_time(row_element):
    """
    Extracts the match time from <div class="date-info ng-star-inserted"><div> 18:45 </div></div>.
    """
    try:
        date_info = row_element.find_element(By.CSS_SELECTOR, "div.date-info.ng-star-inserted")
        raw_time = date_info.text.strip()
        return raw_time if raw_time else "N/A"
    except NoSuchElementException:
        return "N/A"
    except Exception as e:
        logging.error(f"Error parsing match time: {e}")
        return "N/A"

def parse_teams(row_element):
    """
    Extracts home and away team names from something like:
      <div class="w-100">
         <div>HomeTeam</div>
         <div>AwayTeam</div>
      </div>
    """
    try:
        w100_div = row_element.find_element(By.CSS_SELECTOR, "div.w-100")
        divs = w100_div.find_elements(By.TAG_NAME, "div")
        if len(divs) >= 2:
            home = divs[0].text.strip() or "N/A"
            away = divs[1].text.strip() or "N/A"
            return (home, away)
        return ("N/A", "N/A")
    except NoSuchElementException:
        return ("N/A", "N/A")
    except Exception as e:
        logging.error(f"Error parsing team names: {e}")
        return ("N/A", "N/A")

def parse_odds(row_element, driver):
    """
    Extract 1, x, and 2 odds from the <xbet-event-list-item-market> that has
    style="order: 0|2|4" child <div> elements each containing <div class="bet-info odds">.
    """
    odds = {"1": "N/A", "x": "N/A", "2": "N/A"}
    try:
        market_el = row_element.find_element(
            By.CSS_SELECTOR, "xbet-event-list-item-market.market.flex-fill"
        )
        logging.info(f"Found <xbet-event-list-item-market> with text: {market_el.text[:60]}...")

        ordered_divs = market_el.find_elements(By.CSS_SELECTOR, "div[style*='order: ']")
        # Show what .innerText we found for debugging:
        logging.info(
            f"Found {len(ordered_divs)} ordered_div(s): "
            f"{[div.get_attribute('innerText') for div in ordered_divs]}"
        )

        for div_el in ordered_divs:
            style_attr = div_el.get_attribute("style")
            try:
                bet_info = div_el.find_element(By.CSS_SELECTOR, "div.bet-info.odds")
                # Use driver.execute_script(...) with the driver passed as param
                raw_odd = driver.execute_script("return arguments[0].innerText;", bet_info).strip()
                if not raw_odd:
                    raw_odd = "N/A"
            except NoSuchElementException:
                raw_odd = "N/A"

            if "order: 0" in style_attr:
                odds["1"] = raw_odd
            elif "order: 2" in style_attr:
                odds["x"] = raw_odd
            elif "order: 4" in style_attr:
                odds["2"] = raw_odd

    except NoSuchElementException:
        logging.info("No <xbet-event-list-item-market> found (maybe a dummy row).")
    except Exception as e:
        logging.error(f"Error parsing odds: {e}")

    return odds

def get_unique_match_key(row_element, index):
    try:
        unique_id = row_element.get_attribute("data-id")
        if unique_id:
            return unique_id
    except:
        pass
    return f"match_{index}"

def close_all_modals(driver):
    """
    Closes all modals present on the page by clicking on known close buttons.
    """
    try:
        # Find all xbet-modal elements that are visible
        modals = driver.find_elements(By.CSS_SELECTOR, "xbet-modal")
        visible_modals = [modal for modal in modals if modal.is_displayed()]
        logging.info(f"Found {len(visible_modals)} visible modal(s) to close.")
        
        for modal in visible_modals:
            try:
                # Attempt to find and click the 'X' close icon
                close_icons = modal.find_elements(By.CSS_SELECTOR, "div.icon.icon-close")
                for icon in close_icons:
                    if icon.is_displayed() and icon.is_enabled():
                        icon.click()
                        logging.info("Closed a modal using the 'X' icon.")
                        time.sleep(1)  # Wait briefly after closing
                        break  # Move to next modal
            except Exception as e:
                logging.error(f"Error closing modal with 'X' icon: {e}")
            
            try:
                # Attempt to find and click the 'Zatvori' (Close) button
                close_buttons = modal.find_elements(By.XPATH, ".//div[contains(@class, 'btn') and contains(text(), 'Zatvori')]")
                for btn in close_buttons:
                    if btn.is_displayed() and btn.is_enabled():
                        btn.click()
                        logging.info("Closed a modal using the 'Zatvori' button.")
                        time.sleep(1)
                        break  # Move to next modal
            except Exception as e:
                logging.error(f"Error closing modal with 'Zatvori' button: {e}")
        
        # Optionally, wait until all modals are closed
        WebDriverWait(driver, 5).until_not(
            EC.visibility_of_any_elements_located((By.CSS_SELECTOR, "xbet-modal"))
        )
        logging.info("All modals closed successfully.")
    
    except TimeoutException:
        logging.info("No modals found to close.")
    except Exception as e:
        logging.error(f"Unexpected error while trying to close modals: {e}")

# --------------------- #
# 3. Main Scraper Logic #
# --------------------- #

def run():
    web = "https://www.volcanobet.ba/sport-v2/prematch/events"

    os.makedirs('log', exist_ok=True)
    logging.basicConfig(
        filename='log/scraper_volcanobet.log',
        filemode='w',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Starting Volcanobet scraper (combined).")

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--headless")  # Uncomment if you want to run headlessly
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-gpu')          # Suppress GPU-related warnings
    options.add_argument('--disable-webgl')        # Suppress WebGL-related warnings
    options.add_argument('--log-level=3')          # Suppress logging
    options.add_argument('--no-sandbox')           # Bypass OS security model, useful for some environments
    options.add_argument('--disable-dev-shm-usage')# Overcome limited resource problems

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
        # 1) Navigate
        driver.get(web)
        logging.info(f"Navigated to {web}")
        time.sleep(2)

        # 2) Close all modals if present
        close_all_modals(driver)

        # 3) Handle cookie consent
        try:
            cookie_modal = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.sticky-cookie-info.flex-column.w-100.mx-3")
                )
            )
            accept_button = cookie_modal.find_element(
                By.CSS_SELECTOR, "button.btn.btn-primary.margin-fix"
            )
            accept_button.click()
            logging.info("Accepted the cookie consent.")
            time.sleep(1)
        except TimeoutException:
            logging.warning("Cookie consent modal not found or already accepted.")
        except NoSuchElementException:
            logging.warning("Accept button not found in cookie consent modal.")
        except Exception as e:
            logging.error(f"Error handling cookie consent modal: {e}")

        # 4) Click on the "Sve" date button
        try:
            date_picker = driver.find_element(By.TAG_NAME, "xbet-sport-date-picker")
            sve_button = date_picker.find_element(
                By.XPATH, ".//div[@class='item ng-star-inserted']/span[text()='Sve']"
            )
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, ".//div[@class='item ng-star-inserted']/span[text()='Sve']")
                )
            )
            sve_button.click()
            logging.info("Clicked on the 'Sve' date button.")
            time.sleep(1)
        except TimeoutException:
            logging.error("Could not find or click the 'Sve' date button.")
        except Exception as e:
            logging.error(f"Error clicking 'Sve' button: {e}")

        # 5) Click "Fudbal"
        try:
            fudbal_item = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//div[contains(@class, 'sport-item')]//div[contains(@class, 'flex-fill') and text()='Fudbal']",
                    )
                )
            )
            fudbal_item.click()
            logging.info("Clicked on 'Fudbal'.")
            time.sleep(2)
        except TimeoutException:
            logging.error("Could not find or click the 'Fudbal' item.")
        except Exception as e:
            logging.error(f"Error clicking 'Fudbal': {e}")

        # 6) Scrollable container
        try:
            scroll_container = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.scroller"))
            )
            logging.info("Scrollable container found for matches.")
        except TimeoutException:
            logging.error("Did not find scrollable container.")
            return
        except Exception as e:
            logging.error(f"Error locating scrollable container: {e}")
            return

        # 7) Scroll & parse
        all_matches = []
        processed_keys = set()
        target_count = 400
        scroll_attempts = 0
        max_scroll_attempts = 20

        last_height = driver.execute_script(
            "return arguments[0].scrollHeight;", scroll_container
        )

        while len(all_matches) < target_count and scroll_attempts < max_scroll_attempts:
            # Scroll down
            try:
                driver.execute_script("arguments[0].scrollTop += 600;", scroll_container)
                logging.info(f"Scrolled down by 600. Attempt {scroll_attempts+1}/{max_scroll_attempts}")
                time.sleep(2)
            except Exception as e:
                logging.error(f"Scrolling error: {e}")
                break

            new_height = driver.execute_script("return arguments[0].scrollHeight;", scroll_container)
            if new_height == last_height:
                scroll_attempts += 1
                logging.info(f"No new content. scroll_attempts={scroll_attempts}/{max_scroll_attempts}")
                if scroll_attempts >= max_scroll_attempts:
                    logging.info("Max scroll attempts reached. Stopping scroll.")
                    break
            else:
                last_height = new_height
                scroll_attempts = 0

            # Gather match rows
            row_elements = driver.find_elements(By.CSS_SELECTOR, "xbet-event-list-item")
            logging.info(f"Detected {len(row_elements)} row(s) so far.")

            for idx, row_el in enumerate(row_elements, start=1):
                if len(all_matches) >= target_count:
                    break

                # Unique match key
                key = get_unique_match_key(row_el, idx)
                if key in processed_keys:
                    continue
                processed_keys.add(key)

                # Scroll row into view
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", row_el)
                    time.sleep(0.3)
                except Exception as e:
                    logging.warning(f"Could not scroll row {idx} into view: {e}")

                # Parse
                match_time = parse_time(row_el)
                home_team, away_team = parse_teams(row_el)
                odds_dict = parse_odds(row_el, driver)

                match_data = {
                    "key": key,
                    "time": match_time,
                    "home": home_team,
                    "away": away_team,
                    "1": odds_dict["1"],
                    "x": odds_dict["x"],
                    "2": odds_dict["2"],
                }

                all_matches.append(match_data)
                logging.info(
                    f"Scraped {len(all_matches)} => {home_team} vs {away_team} @ {match_time}, "
                    f"1={odds_dict['1']} x={odds_dict['x']} 2={odds_dict['2']}"
                )

        logging.info(f"Total matches collected: {len(all_matches)}")

        # Create DataFrame
        df = pd.DataFrame(all_matches)
        logging.info("DataFrame created. Sample:\n" + str(df.head(5)))

        # Save
        try:
            os.makedirs("data", exist_ok=True)
            df.to_excel("data/volcanobet.xlsx", index=False)
            logging.info("Saved to data/volcanobet.xlsx")

            os.makedirs("pickle_data", exist_ok=True)
            with open("pickle_data/volcanobetbin.pkl", "wb") as f:
                pickle.dump(df, f)
            logging.info("Pickle saved to pickle_data/volcanobetbin.pkl")
        except Exception as e:
            logging.error(f"Error saving data: {e}")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")

    finally:
        try:
            driver.quit()
            logging.info("Driver closed.")
        except:
            pass

if __name__ == "__main__":
    run()
