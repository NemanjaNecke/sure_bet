from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import pickle
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
import logging
import os


def run():
    # **Configure Logging Inside the Run Function**
    log_dir = 'log'
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_dir, 'scraperwwin.log'),
        filemode='w',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Starting scraper_maxbet_1")
    web = "https://wwin.com/sports/#/2"
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

    # Lists to store data
    lige = []
    mecevihome = []
    meceviaway = []
    kvote_1 = []
    kvote_x = []
    kvote_2 = []
    kvote_1x = []
    kvote_x2 = []
    kvote_12 = []
    match_dates = []  # Each entry will be a formatted datetime string

    driver.get(web)

    # Define weekdays in the language used by the website (assuming English)
    weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    def parse_date(day_text, time_text):
        today = datetime.now()

        if day_text.lower() == "danas":  # Today
            match_date = today.date()
        elif day_text.lower() == "sutra":  # Tomorrow
            match_date = (today + timedelta(days=1)).date()
        elif day_text in weekdays:  # English weekday names
            current_weekday = today.weekday()  # 0=Mon, 6=Sun
            target_weekday = weekdays.index(day_text)
            days_ahead = (target_weekday - current_weekday) % 7
            match_date = (today + timedelta(days=days_ahead)).date()
        else:
            # If it's not a recognized format, return None
            logging.error(f"Unknown day_text format: {day_text}")
            return None

        # Combine match_date with time_text
        try:
            match_time = datetime.strptime(time_text, "%H:%M").time()
            return datetime.combine(match_date, match_time)
        except ValueError as e:
            logging.error(f"Error parsing time: {time_text}, {e}")
            return None

    def league_list():
        """
        Returns all league/match elements in .sport-events__wrapper
        so we can iterate over them.
        """
        try:
            wrapper = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".sport-events__wrapper"))
            )
            lista_liga = wrapper.find_elements(By.XPATH, "./div")
            logging.info(f"Found {len(lista_liga)} elements in sport-events__wrapper.")
            return lista_liga
        except Exception as e:
            logging.error(f"Error finding sport-events__wrapper: {e}")
            return []

    def pull_odds():
        """
        For each league/match element, extract league name, home/away teams, odds, and date/time.
        """
        current_league = None
        elements = league_list()
        logging.info("Starting to parse league and match data.")

        for idx, element in enumerate(elements):
            class_attr = element.get_attribute("class")
            if "headmarket__v2" in class_attr:
                # It's a league header
                try:
                    temp = element.find_element(By.CSS_SELECTOR, ".headmarket__title--overflow__v2__tournament")
                    current_league = temp.text
                    logging.info(f"Current league set to: {current_league}")
                except Exception as e:
                    logging.error(f"Error extracting league name: {e}")
                    current_league = "Unknown League"
            elif "live-match__hov" in class_attr and "single-match__prematch" in class_attr:
                # It's a match
                try:
                    # Home & Away Teams
                    home = element.find_element(By.CSS_SELECTOR, ".live-name-v__home").text
                    away = element.find_element(By.CSS_SELECTOR, ".live-name-v__away").text
                    mecevihome.append(home)
                    meceviaway.append(away)
                    logging.debug(f"Match found: {home} vs {away}")
                except Exception as e:
                    logging.error(f"Error extracting team names: {e}")
                    mecevihome.append("")
                    meceviaway.append("")

                # League
                lige.append(current_league if current_league else "Unknown League")

                # Date/Time
                try:
                    day_elem = element.find_element(By.CSS_SELECTOR, ".live-match-date__day")
                    time_elem = element.find_element(By.CSS_SELECTOR, ".live-match-date__time")
                    day_text = day_elem.text.strip()
                    time_text = time_elem.text.strip()
                    match_datetime = parse_date(day_text, time_text)
                    formatted_datetime = match_datetime.strftime("%Y-%m-%d %H:%M:%S") if match_datetime else ""
                    match_dates.append(formatted_datetime)
                except Exception as e:
                    logging.error(f"Error extracting date/time: {e}")
                    match_dates.append("")

                # Odds
                try:
                    temp_odds = element.find_elements(By.CSS_SELECTOR, ".sport-bet__odd.activ")
                    # Initialize with empty strings
                    odds = [''] * 6
                    for i in range(min(len(temp_odds), 6)):
                        odds[i] = temp_odds[i].text.strip()
                    kvote_1.append(odds[0])
                    kvote_x.append(odds[1])
                    kvote_2.append(odds[2])
                    kvote_1x.append(odds[3])
                    kvote_x2.append(odds[4])
                    kvote_12.append(odds[5])
                    logging.debug(f"Odds extracted: {odds}")
                except Exception as e:
                    logging.error(f"Error extracting odds: {e}")
                    kvote_1.append('')
                    kvote_x.append('')
                    kvote_2.append('')
                    kvote_1x.append('')
                    kvote_x2.append('')
                    kvote_12.append('')

        logging.info("Finished parsing league and match data.")

    def change_date(date_index):
        """
        Opens the calendar, selects a day, chooses the first sport (football),
        then calls pull_odds() to scrape the data.
        """
        try:
            # Open calendar
            calendar_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".sports__last-minute__calendar"))
            )
            calendar_button.click()
            logging.info(f"Opened calendar to select date index {date_index}.")
        except Exception as e:
            logging.error(f"Error clicking calendar: {e}")
            return

        try:
            # Wait for calendar days to be clickable
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".msports__calendar--day.active"))
            )
            lista_dana = driver.find_elements(By.CSS_SELECTOR, ".msports__calendar--day.active")
            if len(lista_dana) > date_index:
                lista_dana[date_index].click()
                logging.info(f"Selected date index {date_index}.")
            else:
                logging.warning(f"Day index {date_index} not found in calendar. Skipping...")
                return
        except Exception as e:
            logging.error(f"Error selecting date: {e}")
            return

        # Select sport (football, presumably the first item)
        try:
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#sport-list .sport-sidebar__list"))
            )
            lista_sportova = driver.find_elements(By.CSS_SELECTOR, "#sport-list .sport-sidebar__list")
            if lista_sportova:
                lista_sportova[0].click()
                logging.info("Selected first sport in sport list.")
            else:
                logging.warning("No sports found to select.")
        except Exception as e:
            logging.error(f"Error selecting sport: {e}")
            return

        # Wait for matches to load
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".sport-events__wrapper"))
            )
            logging.info("Matches loaded after date and sport selection.")
        except Exception as e:
            logging.error(f"Error waiting for matches to load: {e}")
            return

        # Now pull the odds, teams, dates
        pull_odds()

    # Iterate over desired date indices (e.g., 1 to 5)
    for date_idx in range(1, 9):
        logging.info(f"Processing date index {date_idx}.")
        change_date(date_idx)
        # Optional: Add a short wait between date selections
        time.sleep(1)

    # Build dictionary for our DataFrame
    kompletna_ponuda = {
        "time": match_dates,        # <--- new column
        "home": mecevihome,
        "away": meceviaway,
        "1": kvote_1,
        "x": kvote_x,
        "2": kvote_2,
        "1x": kvote_1x,
        "x2": kvote_x2,
        "12": kvote_12,
        "league": lige  # Optionally include league information
    }

    # Make sure all lists have the same length
    array_lengths = [len(v) for v in kompletna_ponuda.values()]
    if len(set(array_lengths)) != 1:
        max_length = max(array_lengths)
        for key, value in kompletna_ponuda.items():
            if len(value) < max_length:
                value.extend([''] * (max_length - len(value)))
        logging.warning("Extended shorter lists to match maximum length.")

    # Convert to DataFrame
    df = pd.DataFrame.from_dict(kompletna_ponuda)
    logging.info(f"DataFrame created with {len(df)} records.")
    logging.info(df.head())  # Debug print (optional)

    # Ensure output directories exist
    os.makedirs('data', exist_ok=True)
    os.makedirs('pickle_data', exist_ok=True)

    # Save to Excel and pickle
    try:
        df.to_excel('data/takmicenjewwin.xlsx', index=False)
        with open('pickle_data/wwinbin.pkl', 'wb') as output:
            pickle.dump(df, output)
        logging.info(f"Saved to data/takmicenjewwin.xlsx and pickle_data/wwinbin")
    except Exception as e:
        logging.error(f"Error saving data: {e}")

    driver.quit()
    logging.info("WebDriver closed successfully.")


if __name__ == "__main__":
    run()
