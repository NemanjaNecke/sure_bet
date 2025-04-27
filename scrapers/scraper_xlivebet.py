import logging
import os
import time
import pickle
import pandas as pd
import re
import random
import datetime
import undetected_chromedriver as uc  # Importing undetected-chromedriver
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

# ---------------------------- Configuration ---------------------------- #

# Configure logging
LOG_DIR = 'log'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'scraper_xlivebet.log'),
    filemode='w',
    level=logging.DEBUG,  # Set to DEBUG for detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Directory to save scraped data
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

# ---------------------------- Helper Functions ---------------------------- #

def parse_teams(teams_str):
    """
    Parse the teams string to extract home and away team names.
    Expected format:
        'TeamA - TeamB LIVE' --> ('TeamA', 'TeamB')
    """
    try:
        teams_clean = re.sub(r'LIVE', '', teams_str).strip()
        if '-' in teams_clean:
            home, away = teams_clean.split('-', 1)
            home = home.strip()
            away = away.strip()
        else:
            home, away = "N/A", "N/A"
        return home, away
    except Exception as e:
        logging.error(f"Error parsing teams from '{teams_str}': {e}")
        return "N/A", "N/A"

def scrape_matches(driver):
    """
    Parses the main match table on the page.
    Returns a list of dictionaries, each representing a match.
    """
    matches_data = []
    try:
        match_containers = WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, "//div[contains(@class, 'tg__match_item')]")
            )
        )
        logging.info(f"Found {len(match_containers)} match containers.")
        
        for idx, container in enumerate(match_containers, start=1):
            try:
                logging.debug(f"Processing match container {idx}")
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", container)
                time.sleep(random.uniform(0.1, 0.3))
                
                try:
                    match_no = container.find_element(
                        By.XPATH, ".//div[contains(@class, 'tg--ns')]/div[contains(@class, 'tg--mar-r-8')][1]"
                    ).text.strip()
                    logging.debug(f"Extracted match_no: {match_no}")
                except NoSuchElementException:
                    match_no = "N/A"
                    logging.warning(f"Match number not found for container {idx}.")
                
                teams_elements = container.find_elements(
                    By.XPATH, ".//div[contains(@class, 'tg__teams')]//div[contains(@class, 'prematch_name')]"
                )
                if len(teams_elements) >= 2:
                    home_team = teams_elements[0].get_attribute("title").strip()
                    away_team = teams_elements[1].get_attribute("title").strip()
                    logging.debug(f"Extracted teams: {home_team} vs {away_team}")
                else:
                    home_team, away_team = "N/A", "N/A"
                    logging.warning(f"Teams not found for match_no: {match_no}")
                
                try:
                    kickoff_text = container.find_element(
                        By.XPATH, ".//div[contains(@class, 'tg--ns')]/div[contains(@class, 'tg--mar-r-8')][3]"
                    ).text.strip()
                    match_time = kickoff_text
                    logging.debug(f"Extracted match_time: {match_time}")
                except NoSuchElementException:
                    match_time = "N/A"
                    logging.warning(f"Time not found for match_no: {match_no}")
                
                odds_elements = container.find_elements(
                    By.XPATH, ".//div[contains(@class, 'prematch_event_odds_container')]//a"
                )
                odds_dict = {"1": "N/A", "x": "N/A", "2": "N/A"}
                for odd in odds_elements:
                    try:
                        odd_name = odd.find_element(
                            By.XPATH, ".//div[contains(@class, 'tg__match_item_odd_name')]"
                        ).get_attribute("title").strip().lower()
                        if odd_name == '1':
                            key = '1'
                        elif odd_name == 'x':
                            key = 'x'
                        elif odd_name == '2':
                            key = '2'
                        else:
                            continue
                        odd_value = odd.find_element(
                            By.XPATH, ".//div[contains(@class, 'prematch_stake_odd_factor')]"
                        ).text.strip()
                        odds_dict[key] = odd_value
                        logging.debug(f"Extracted odd {key}: {odd_value}")
                    except NoSuchElementException:
                        continue
                
                match_info = {
                    "match_no": match_no,
                    "home": home_team,
                    "away": away_team,
                    "time": match_time,
                    "1": odds_dict.get("1", "N/A"),
                    "x": odds_dict.get("x", "N/A"),
                    "2": odds_dict.get("2", "N/A"),
                }
                
                if home_team != "N/A" and away_team != "N/A":
                    matches_data.append(match_info)
                    logging.info(
                        f"Scraped {match_no}: {home_team} vs {away_team} @ {match_time}, "
                        f"1={odds_dict['1']} x={odds_dict['x']} 2={odds_dict['2']}"
                    )
                else:
                    logging.info(f"Skipped match_no {match_no} due to missing team information.")
            
            except Exception as ex_row:
                logging.error(f"Error parsing match container {idx}: {ex_row}")
                continue

    except TimeoutException:
        logging.error("Match containers not found within the given time.")
    except NoSuchElementException:
        logging.error("No match containers found on the page.")
    except Exception as e:
        logging.error(f"Unexpected error in scrape_matches: {e}")
    
    return matches_data

def safe_click(driver, element, label="(unknown)"):
    """
    Attempts to click an element using various methods.
    """
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(random.uniform(0.2, 0.5))
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

def switch_to_iframe_containing_element(driver, element_xpath):
    """
    Switches to the iframe containing the desired element.
    """
    try:
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        logging.info(f"Found {len(iframes)} iframes on the page.")
        for index, iframe in enumerate(iframes, start=1):
            try:
                driver.switch_to.frame(iframe)
                logging.info(f"Switched to iframe {index}.")
                elements = driver.find_elements(By.XPATH, element_xpath)
                if elements:
                    logging.info(f"Element found in iframe {index}.")
                    return True
                else:
                    logging.info(f"Element not found in iframe {index}. Switching back.")
                    driver.switch_to.default_content()
            except Exception as e:
                logging.error(f"Error switching to iframe {index}: {e}")
                driver.switch_to.default_content()
                continue
        logging.error("Element not found in any iframe.")
        driver.switch_to.default_content()
        return False
    except Exception as e:
        logging.error(f"Error during iframe handling: {e}")
        return False

def add_stealth(driver):
    """
    Adds stealth features to the Selenium driver.
    """
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                      get: () => undefined
                    });
                    window.navigator.chrome = {
                        runtime: {},
                    };
                    Object.defineProperty(navigator, 'plugins', {
                      get: () => [1, 2, 3, 4, 5],
                    });
                    Object.defineProperty(navigator, 'languages', {
                      get: () => ['en-US', 'en'],
                    });
                """
            }
        )
        logging.info("Applied stealth modifications to the WebDriver.")
    except Exception as e:
        logging.error(f"Error applying stealth modifications: {e}")

def is_element_visible(element):
    """
    Checks if the element is visible on the page.
    """
    try:
        return element.is_displayed()
    except Exception as e:
        logging.error(f"Error checking visibility of element: {e}")
        return False

# ---------------------------- Main Scraper Function ---------------------------- #

def run():
    """
    Main function to execute the scraping process.
    """
    print("Starting scraper_xlivebet...")  # Print to console so we know it started
    url = "https://xlivebet.ba/Sport/pre-match/?sport=1"
    
    options = uc.ChromeOptions()
    options.add_argument("--start-maximized")
    # Uncomment below to run headless if needed:
    # options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
    
    try:
        driver = uc.Chrome(options=options)
        logging.info("Undetected WebDriver initialized successfully.")
    except WebDriverException as e:
        logging.error(f"Error initializing undetected WebDriver: {e}", exc_info=True)
        return
    
    try:
        driver.get(url)
        logging.info(f"Navigated to {url}")
        add_stealth(driver)
        time.sleep(random.uniform(0.3, 0.5))
        
        def close_overlays(driver):
            try:
                consent_buttons = driver.find_elements(By.XPATH, "//div[@class='cookies-close']")
                for button in consent_buttons:
                    try:
                        button.click()
                        logging.info("Closed an overlay by clicking on consent button.")
                        time.sleep(random.uniform(0.2, 0.5))
                    except Exception as e:
                        logging.warning(f"Failed to click on consent button: {e}")
                modal_close_buttons = driver.find_elements(By.XPATH, "//button[contains(@class, 'modal-close')]")
                for button in modal_close_buttons:
                    try:
                        button.click()
                        logging.info("Closed an overlay by clicking on modal close button.")
                        time.sleep(random.uniform(0.2, 0.5))
                    except Exception as e:
                        logging.warning(f"Failed to click on modal close button: {e}")
            except Exception as e:
                logging.error(f"Error while trying to close overlays: {e}")
        
        close_overlays(driver)
        
        LEFT_MENU_XPATH = "//ul[contains(@class, 'sports_list') and contains(@class, 'left_sport_list')]"
        
        try:
            WebDriverWait(driver, 20).until(
                EC.visibility_of_element_located((By.XPATH, LEFT_MENU_XPATH))
            )
            left_menu = driver.find_element(By.XPATH, LEFT_MENU_XPATH)
            logging.info("Located the left menu in default content.")
        except TimeoutException:
            logging.warning("Left menu not found in default content. Attempting to locate within iframes.")
            if switch_to_iframe_containing_element(driver, LEFT_MENU_XPATH):
                try:
                    left_menu = driver.find_element(By.XPATH, LEFT_MENU_XPATH)
                    logging.info("Located the left menu within an iframe.")
                except NoSuchElementException:
                    logging.error("Left menu not found even after switching to iframes.")
                    driver.quit()
                    return
            else:
                logging.error("Left menu not found within any iframes.")
                driver.quit()
                return
        
        time.sleep(random.uniform(0.2, 0.3))
        
    except TimeoutException:
        logging.error(f"Left menu not found within the given time using selector '{LEFT_MENU_XPATH}'.")
        driver.quit()
        return
    except Exception as e:
        logging.error(f"Error navigating to {url}: {e}", exc_info=True)
        driver.quit()
        return

    all_matches = []

    try:
        fudbal_element = left_menu.find_element(
            By.XPATH, 
            ".//li/a[span[starts-with(normalize-space(text()), 'Fudbal')]]"
        )
        logging.info("Located the first 'Fudbal' menu item.")
    except NoSuchElementException:
        logging.error("No 'Fudbal' menu item found. Exiting.")
        driver.quit()
        return
    except Exception as e:
        logging.error(f"Error locating 'Fudbal' menu item: {e}", exc_info=True)
        driver.quit()
        return

    try:
        submenu_container = fudbal_element.find_element(By.XPATH, "./following-sibling::ul[@class='champ_list']")
        if is_element_visible(submenu_container):
            logging.info("Submenu for 'Fudbal' is already visible. No need to click.")
            submenu_visible = True
        else:
            logging.info("Submenu for 'Fudbal' is not visible. Proceeding to click to expand.")
            submenu_visible = False
    except NoSuchElementException:
        logging.error("Submenu <ul class='champ_list'> not found inside the 'Fudbal' menu item.")
        driver.quit()
        return
    except Exception as e:
        logging.error(f"Error locating submenu for 'Fudbal': {e}", exc_info=True)
        driver.quit()
        return

    if not submenu_visible:
        logging.info(f"Attempting to click on the 'Fudbal' menu item: {fudbal_element.text.strip()}.")
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", fudbal_element)
            time.sleep(random.uniform(0.2, 0.3))
        except Exception as e_scroll:
            logging.warning(f"Failed to scroll the 'Fudbal' menu item into view: {e_scroll}")
        if safe_click(driver, fudbal_element, label="'Fudbal' Menu Item"):
            logging.info("Clicked on the 'Fudbal' menu item successfully.")
            time.sleep(random.uniform(0.2, 0.3))
            try:
                WebDriverWait(driver, 10).until(
                    EC.visibility_of(submenu_container)
                )
                logging.info("Submenu <ul class='champ_list'> for 'Fudbal' is now visible.")
            except TimeoutException:
                logging.error("Submenu <ul class='champ_list'> did not become visible for the 'Fudbal' menu item.")
                driver.quit()
                return
            except Exception as e:
                logging.error(f"Error waiting for submenu to become visible: {e}", exc_info=True)
                driver.quit()
                return
        else:
            logging.error("Failed to click on the 'Fudbal' menu item. Exiting.")
            driver.quit()
            return
    else:
        logging.info("Submenu is already visible. Proceeding without clicking 'Fudbal'.")

    try:
        WebDriverWait(driver, 10).until(
            EC.visibility_of(submenu_container)
        )
        logging.info("Located submenu <ul class='champ_list'> for 'Fudbal'.")
        first_level_submenus = submenu_container.find_elements(By.XPATH, "./li[contains(@class, 'champ_container')]")
        logging.info(f"Found {len(first_level_submenus)} first-level submenu <li class='champ_container'> items under 'Fudbal'.")
    except NoSuchElementException:
        logging.error("Submenu <ul class='champ_list'> not found inside the 'Fudbal' menu item.")
        driver.quit()
        return
    except TimeoutException:
        logging.error("Submenu <ul class='champ_list'> did not become visible for the 'Fudbal' menu item.")
        driver.quit()
        return
    except Exception as e:
        logging.error(f"Error locating submenu items for 'Fudbal': {e}", exc_info=True)
        driver.quit()
        return

    for i, first_level_submenu in enumerate(first_level_submenus, start=1):
        try:
            try:
                first_level_text = first_level_submenu.find_element(By.XPATH, ".//span").get_attribute("title").strip()
                logging.info(f"\nProcessing first-level submenu '{first_level_text}' ({i}/{len(first_level_submenus)}).")
            except NoSuchElementException:
                first_level_text = "N/A"
                logging.warning(f"First-level submenu item {i} does not have a <span> with title attribute.")
            
            if safe_click(driver, first_level_submenu, label=f"First-level Submenu '{first_level_text}'"):
                logging.info(f"Clicked on first-level submenu '{first_level_text}' successfully.")
                time.sleep(random.uniform(0.2, 0.3))
                try:
                    second_level_submenu_container = first_level_submenu.find_element(By.XPATH, "./ul")
                    logging.info(f"Located second-level submenu <ul> for '{first_level_text}'.")
                    WebDriverWait(driver, 10).until(
                        EC.visibility_of(second_level_submenu_container)
                    )
                    second_level_submenus = second_level_submenu_container.find_elements(By.XPATH, "./li[contains(@class, 'leftSLLi')]")
                    logging.info(f"Found {len(second_level_submenus)} second-level submenu <li class='leftSLLi'> items under '{first_level_text}'.")
                except NoSuchElementException:
                    logging.error(f"Second-level submenu <ul> not found inside first-level submenu '{first_level_text}'.")
                    continue
                except TimeoutException:
                    logging.error(f"Second-level submenu <ul> did not become visible for first-level submenu '{first_level_text}'.")
                    continue
                except Exception as e:
                    logging.error(f"Error locating second-level submenu items for '{first_level_text}': {e}", exc_info=True)
                    continue

                for j, second_level_submenu in enumerate(second_level_submenus, start=1):
                    try:
                        try:
                            second_level_text = second_level_submenu.find_element(By.XPATH, ".//span").get_attribute("title").strip()
                            logging.info(f"  Processing second-level submenu '{second_level_text}' ({j}/{len(second_level_submenus)}).")
                        except NoSuchElementException:
                            second_level_text = "N/A"
                            logging.warning(f"Second-level submenu item {j} under '{first_level_text}' does not have a <span> with title attribute.")
                        
                        if safe_click(driver, second_level_submenu, label=f"Second-level Submenu '{second_level_text}'"):
                            logging.info(f"  Clicked on second-level submenu '{second_level_text}' successfully.")
                            time.sleep(random.uniform(0.2, 0.3))
                            matches = scrape_matches(driver)
                            logging.info(f"  Scraped {len(matches)} matches from subleague '{second_level_text}'.")
                            all_matches.extend(matches)
                        else:
                            logging.warning(f"  Failed to click on second-level submenu '{second_level_text}'.")
                            continue
                    except Exception as e_second:
                        logging.error(f"  Error processing second-level submenu '{second_level_submenu.text}': {e_second}", exc_info=True)
                        continue
            else:
                logging.warning(f"Failed to click on first-level submenu '{first_level_text}'.")
                continue
        except Exception as e_first:
            logging.error(f"Error processing first-level submenu '{first_level_submenu.text}': {e_first}", exc_info=True)
            continue

    logging.info("\nCompleted processing all first-level submenu items.")

    scroll_attempts = 0
    max_scroll_attempts = 3
    try:
        offer_scroll_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "offerScroll"))
        )
        last_height = driver.execute_script("return arguments[0].scrollHeight;", offer_scroll_element)
        logging.info(f"Initial scroll height: {last_height}")
    except Exception as e:
        logging.error(f"Error locating 'offerScroll' element: {e}", exc_info=True)
        last_height = 0

    while scroll_attempts < max_scroll_attempts:
        try:
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", offer_scroll_element)
            logging.info("Scrolled 'offerScroll' container to the bottom.")
            time.sleep(random.uniform(0.3, 0.7))
            new_height = driver.execute_script("return arguments[0].scrollHeight;", offer_scroll_element)
            logging.info(f"New scroll height: {new_height}")
        except Exception as e_scroll:
            logging.error(f"Error scrolling 'offerScroll' container: {e_scroll}", exc_info=True)
            break

        if new_height == last_height:
            scroll_attempts += 1
            logging.info(f"No new content. scroll_attempts={scroll_attempts}/{max_scroll_attempts}")
            if scroll_attempts >= max_scroll_attempts:
                logging.info("Max scroll attempts reached. Stopping scroll.")
                break
        else:
            last_height = new_height
            scroll_attempts = 0

    try:
        matches = scrape_matches(driver)
        logging.info(f"Final scrape: Scraped {len(matches)} matches.")
        all_matches.extend(matches)
    except Exception as e:
        logging.error(f"Error during match scraping: {e}", exc_info=True)

    # ----------------- Driver Closing Modification ----------------- #
    try:
        driver.quit()
        logging.info("WebDriver closed successfully.")
    except Exception as e:
        logging.error(f"Error closing WebDriver: {e}", exc_info=True)
    finally:
        driver = None
        import gc
        gc.collect()
    # ----------------- End of Modification ----------------- #

    if all_matches:
        try:
            df = pd.DataFrame(all_matches, columns=["match_no", "home", "away", "time", "1", "x", "2"])
            logging.info(f"DataFrame created with {len(df)} rows.")
            df.drop_duplicates(subset=["match_no"], inplace=True)
            logging.info(f"DataFrame after removing duplicates: {len(df)} rows.")
            os.makedirs(DATA_DIR, exist_ok=True)
            excel_path = os.path.join(DATA_DIR, "takmicenje_xlivebet.xlsx")
            df.to_excel(excel_path, index=False)
            logging.info(f"Data saved to {excel_path}")
            os.makedirs("pickle_data", exist_ok=True)
            pickle_path = os.path.join("pickle_data", "xlivebetbin.pkl")
            with open(pickle_path, "wb") as f:
                pickle.dump(df, f)
            logging.info(f"Data pickled to {pickle_path}")
        except Exception as e:
            logging.error(f"Error saving data: {e}", exc_info=True)
    else:
        logging.info("No matches were scraped.")

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.error("Unhandled exception in scraper_xlivebet", exc_info=True)
        print("Unhandled exception in scraper_xlivebet; check log for details.")
