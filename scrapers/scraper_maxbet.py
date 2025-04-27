import os
import time
import pickle
import logging
import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementNotInteractableException,
    WebDriverException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


def parse_maxbet_date(raw_time: str):
    """
    Given a string like '21.01 16:30', parse to Python datetime with current year.
    Return None on failure.
    """
    raw_time = (raw_time or "").strip()
    if not raw_time:
        return None
    try:
        current_year = datetime.datetime.now().year
        full_str = f"{raw_time} {current_year}"
        return datetime.datetime.strptime(full_str, "%d.%m %H:%M %Y")
    except Exception as e:
        logging.warning(f"Could not parse date '{raw_time}': {e}")
        return None


def should_skip_league(league_label: str) -> bool:
    """
    If we want to skip certain leagues (like 'Bonus odds Soccer'),
    define skip logic here.
    """
    skip_phrases = ["Bonus odds"]
    for phrase in skip_phrases:
        if phrase.lower() in league_label.lower():
            return True
    return False


def run():
    # --- Logging Setup ---
    os.makedirs("log", exist_ok=True)
    logging.basicConfig(
        filename="log/scraper_maxbet.log",
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info("Starting MaxBet league-by-league scraper (no collapse).")

    # --- WebDriver Init ---
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--headless")  # if desired
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        logging.info("WebDriver initialized.")
    except WebDriverException as e:
        logging.error(f"Error initializing WebDriver: {e}")
        return

    wait = WebDriverWait(driver, 15)
    url = "https://www.maxbet.ba/en/home"

    # --- Navigate ---
    try:
        driver.get(url)
        logging.info(f"Navigated to {url}")
        time.sleep(2)
    except Exception as e:
        logging.error(f"Error navigating to {url}: {e}")
        driver.quit()
        return

    # --- Accept cookies if present ---
    try:
        cookie_btn = driver.find_element(By.CSS_SELECTOR, "ds-cookies-consent-modal ion-button")
        cookie_btn.click()
        logging.info("Accepted cookies.")
        time.sleep(1)
    except NoSuchElementException:
        logging.info("No cookie button found.")
    except Exception as e:
        logging.error(f"Error clicking cookie consent button: {e}")

    # --- Find & click Football ---
    try:
        sport_items = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "prematch-sport-item")))
        clicked_football = False
        for item in sport_items:
            label_el = item.find_element(By.TAG_NAME, "ion-label")
            if "Football" in label_el.text.strip():
                item.click()
                logging.info("Clicked on 'Football'.")
                time.sleep(2)
                clicked_football = True
                break
        if not clicked_football:
            logging.warning("No 'Football' item found. Exiting.")
            driver.quit()
            return
    except TimeoutException:
        logging.error("Timeout finding 'Football' category.")
        driver.quit()
        return
    except Exception as e:
        logging.error(f"Error while selecting 'Football': {e}")
        driver.quit()
        return

    # --- Find league items ---
    league_selector = "ds-leagues-accordion ion-item.league-accordion--item"
    try:
        league_items = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, league_selector)))
        logging.info(f"Found {len(league_items)} total leagues.")
    except TimeoutException:
        logging.error("No league items found (Timeout).")
        driver.quit()
        return
    except Exception as e:
        logging.error(f"Error fetching league items: {e}")
        driver.quit()
        return

    # Prepare storage for final results
    all_data = {
        "league": [],
        "time": [],
        "home": [],
        "away": [],
        "1": [],
        "x": [],
        "2": [],
    }

    # For each league, expand -> do small incremental scroll -> parse all <ds-es-match-top>
    for idx, league_item in enumerate(league_items, start=1):
        # Re-locate league element (the DOM can shift each iteration)
        try:
            league_items = driver.find_elements(By.CSS_SELECTOR, league_selector)
            if (idx - 1) >= len(league_items):
                break
            li = league_items[idx - 1]
        except Exception as e:
            logging.warning(f"Error re-locating league item #{idx}: {e}")
            continue

        # Extract league label
        league_label = ""
        try:
            label_el = li.find_element(By.CSS_SELECTOR, ".league-accordion--item-inner ion-label")
            league_label = label_el.text.strip()
        except Exception:
            pass

        # Check skip logic
        if should_skip_league(league_label):
            logging.info(f"Skipping league '{league_label}' as per skip logic.")
            continue

        logging.info(f"League #{idx}: '{league_label}'")

        # Scroll league into view, click to expand
        try:
            driver.execute_script("arguments[0].scrollIntoView(true);", li)
            time.sleep(0.4)
            li.click()
            time.sleep(1.0)
        except ElementNotInteractableException:
            logging.warning(f"Cannot interact with league '{league_label}'. Skipping.")
            continue
        except Exception as e:
            logging.warning(f"Error clicking league '{league_label}': {e}")
            continue

        # Gentle incremental scroll to load more matches
        old_count = 0
        max_scroll_attempts = 12
        for attempt in range(max_scroll_attempts):
            # Count how many matches we see so far for this league
            match_tops = driver.find_elements(By.CSS_SELECTOR, "ds-es-match-top")
            new_count = len(match_tops)
            if new_count > old_count:
                old_count = new_count
                # small scroll increment
                driver.execute_script("window.scrollBy(0, 300);")
                time.sleep(1.0)
            else:
                # No new matches => break
                break

        # Now parse all ds-es-match-top elements belonging to this league
        match_tops = driver.find_elements(By.CSS_SELECTOR, "ds-es-match-top")
        for mtop in match_tops:
            # Check if this match belongs to the same league_label
            try:
                league_el = mtop.find_element(By.CSS_SELECTOR, ".es-match-league")
                if league_el.text.strip() != league_label:
                    continue
            except Exception:
                continue

            # parse date/time
            dt_val = None
            try:
                date_el = mtop.find_element(By.CSS_SELECTOR, ".es-match-kickoff")
                dt_val = parse_maxbet_date(date_el.text)
            except:
                pass

            # parse teams
            hteam, ateam = "", ""
            try:
                teams_els = mtop.find_elements(By.CSS_SELECTOR, ".es-match-teams-item")
                if len(teams_els) >= 2:
                    hteam = teams_els[0].text.strip()
                    ateam = teams_els[1].text.strip()
            except:
                pass

            # parse odds (1, x, 2)
            val1, valx, val2 = "N/A", "N/A", "N/A"
            try:
                odds_btns = mtop.find_elements(By.CSS_SELECTOR, ".odd-btn--odd")
                if len(odds_btns) >= 3:
                    val1 = odds_btns[0].text.strip()
                    valx = odds_btns[1].text.strip()
                    val2 = odds_btns[2].text.strip()
            except:
                pass

            # Store
            all_data["league"].append(league_label)
            all_data["time"].append(dt_val if dt_val else "")
            all_data["home"].append(hteam)
            all_data["away"].append(ateam)
            all_data["1"].append(val1)
            all_data["x"].append(valx)
            all_data["2"].append(val2)

            logging.info(
                f"Scraped -> League={league_label}; Time={dt_val}; "
                f"{hteam} vs {ateam}; 1={val1}, x={valx}, 2={val2}"
            )

        # We do NOT collapse the league. Move on to next.

    driver.quit()
    logging.info("Driver closed.")

    # Build DataFrame & save
    df = pd.DataFrame(all_data)
    logging.info(f"Collected {len(df)} total matches.")

    os.makedirs("data", exist_ok=True)
    df.to_excel("data/takmicenjemaxbet.xlsx", index=False)
    logging.info("Excel saved to data/takmicenjemaxbet.xlsx")

    os.makedirs("pickle_data", exist_ok=True)
    with open("pickle_data/takmicenjemaxbetbin.pkl", "wb") as f:
        pickle.dump(df, f)
    logging.info("Pickle saved to pickle_data/takmicenjemaxbetbin")


if __name__ == "__main__":
    run()
