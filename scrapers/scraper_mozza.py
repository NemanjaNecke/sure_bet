import glob
import os
import pickle
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
import logging
import os


def run():
        # **Configure Logging Inside the Run Function**
    log_dir = 'log'
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_dir, 'scrapermozza.log'),
        filemode='w',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Starting scraper_mozza")
    web = 'https://mozzartbet.ba/en#/date/three_days'
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--start-maximized")
    options.add_argument(f"--user-data-dir=/tmp/chrome_{os.getpid()}")
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

    driver.get(web)
    time.sleep(0.8)
    
    # Close "OneSignal" popup
    accept = driver.find_element("id", "onesignal-slidedown-cancel-button")
    accept.click()

    # Accept GDPR
    accept2 = driver.find_element(By.CSS_SELECTOR, "#gdpr-wrapper-new .gdpr-content .accept-button")
    accept2.click()

    # Scroll & "Load more" setup
    load_more = driver.find_element(By.CSS_SELECTOR, "div.paginator>div.loadMore>div.buttonLoad:last-child")
    remaining = driver.find_element(By.CSS_SELECTOR, ".paginator .remaining")
    remaining_text = remaining.text

    # Click 'Football' tab
    football = driver.find_element(By.XPATH, "//span[text()='Football']")
    football.click()

    # Scroll multiple times
    for let in range(40):
        driver.execute_script(
            "document.getElementsByClassName('footer-logo')[0].scrollIntoView("
            "true,{behavior: 'smooth', block: 'end', inline: 'nearest'})"
        )
        time.sleep(0.3)
    # Try to click "Load more"
    try:
        load_more.click()
    except:
        pass

    # Prepare lists
    league_name = []
    match_datetime_list = []   # We'll store actual Python datetime objects
    match_home = []
    match_away = []
    odds_value_1 = []
    odds_value_x = []
    odds_value_2 = []

    # We’ll handle these recognized day words
    weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    # Optionally, you could also handle local language e.g. "Pon", "Uto", etc.
    # or "Danas", "Sutra".

    def parse_date(date_str):
        """
        Expects something like "Mon 18:30" or "Danas 20:00", etc.
        Splits on space. If the first part is known day_text or "Danas"/"Sutra", 
        we find the date. Then parse HH:MM.
        Returns a datetime object or None if parsing fails.
        """
        today = datetime.now()

        # If there's no space, or something unexpected, handle it
        parts = date_str.strip().split()
        if len(parts) < 2:
            # We expect at least 2 parts, e.g. "Mon" + "18:30"
            return None
        
        day_text = parts[0]
        time_text = parts[-1]  # e.g. "18:30"
        
        # 1) Figure out the date
        if day_text.lower() in ["danas", "today"]:
            match_date = today.date()
        elif day_text.lower() in ["sutra", "tomorrow"]:
            match_date = (today + timedelta(days=1)).date()
        elif day_text in weekdays:
            # English weekday name e.g. "Mon", "Tue"...
            current_weekday = today.weekday()  # 0 = Monday, 6 = Sunday
            target_weekday = weekdays.index(day_text)
            # How many days from 'today' to 'day_text'
            days_ahead = (target_weekday - current_weekday) % 7
            match_date = (today + timedelta(days=days_ahead)).date()
        else:
            # If it's a date like "24.12." or something else
            # we can try direct parsing if you prefer, 
            # but for now let's just return None
            logging.error(f"Unknown day_text format: '{day_text}' in '{date_str}'")
            return None

        # 2) Parse the time (HH:MM)
        try:
            match_t = datetime.strptime(time_text, "%H:%M").time()
        except ValueError:
            logging.error(f"Error parsing time '{time_text}' in '{date_str}'")
            return None

        # Combine date + time
        return datetime.combine(match_date, match_t)

    # Get all competitions
    competitions = driver.find_elements(By.CSS_SELECTOR, ".competition article")

    for i in competitions:
        # League
        leagues_el = i.find_element(By.CSS_SELECTOR, ".infos .leagueName")
        league_name.append(leagues_el.text)

        # Teams
        teams_el = i.find_element(By.CSS_SELECTOR, ".part1 .pairs")
        teams_text = teams_el.text   # e.g. "Mon 18:30\nTeamA\nTeamB" or "TeamA\nTeamB"

        # Time text
        # Sometimes the .time element has "Mon 18:30"
        # Let's extract that
        try:
            match_time_el = i.find_element(By.CSS_SELECTOR, '.part1 .time')
            time_raw = match_time_el.text.strip()  # e.g. "Mon 18:30"
        except:
            time_raw = ""  # fallback

        # Parse the date/time
        dt_obj = parse_date(time_raw)
        match_datetime_list.append(dt_obj)

        # Now parse the teams (like your code does)
        temp = teams_text.split("\n")
        # Typically the first line might be "Mon 18:30" or sometimes time is separate,
        # so watch out if the site changed structure.
        # We'll skip index=0 if we assume it's day/time, but test carefully.
        # In your code, it looks like part1 .pairs might have (time, home, away)
        # or might just have (home, away). Check the actual data.
        # For your code, it looks like you do a mod 3 check. 
        # But let's do something simpler:
        
        # your code does:
        # if index % 3 == 0 => home
        # if index % 3 == 1 => away
        # (the next line is index % 3 == 2 => away, but commented out)
        # This suggests you expect 3 lines per match. Adjust as needed.

        # We'll replicate your approach:
        #   line0: time (?), line1: home, line2: away
        # But you might have only 2 lines if time is separate. 
        # So let's do a safe approach:

        local_home, local_away = "", ""
        if len(temp) >= 3:
            # e.g. temp[0] = "Mon 18:30", temp[1] = "TeamA", temp[2] = "TeamB"
            local_home = temp[1].strip()
            local_away = temp[2].strip()
        elif len(temp) >= 2:
            # e.g. temp[0] = "TeamA", temp[1] = "TeamB"
            local_home = temp[0].strip()
            local_away = temp[1].strip()
        # else we might have just 1 line or none, fallback
        match_home.append(local_home)
        match_away.append(local_away)

        # Extract odds
        odds = i.find_elements(By.CSS_SELECTOR, "div.part2>div.part2wrapper >div.partvar.odds")
        # odds[0] => 1, odds[1] => x, odds[2] => 2
        if len(odds) >= 3:
            odds_value_1.append(odds[0].text)
            odds_value_x.append(odds[1].text)
            odds_value_2.append(odds[2].text)
        else:
            odds_value_1.append("")
            odds_value_x.append("")
            odds_value_2.append("")

    driver.quit()

    # Build final dictionary
    xy = {
        'leagues': league_name, 
        'time': match_datetime_list,
        'home': match_home,
        'away': match_away,
        '1': odds_value_1, 
        'x': odds_value_x,
        '2': odds_value_2
    }

    # Check lengths
    array_lengths = [len(v) for v in xy.values()]
    if not all(l == array_lengths[0] for l in array_lengths):
        logging.error("Warning: arrays have different lengths!")
        max_len = max(array_lengths)
        for k, v in xy.items():
            while len(v) < max_len:
                v.append("")  # pad with empty

    df = pd.DataFrame.from_dict(xy)
    logging.info(df.head())

    # Save to Excel & Pickle
    df.to_excel('data/takmicenjemozza.xlsx', index=False)
    with open('pickle_data/takmicenjemozzabin.pkl', 'wb') as output:
        pickle.dump(df, output)
    logging.info("Succesfully saved to data/takmicenjemozza.xlsx and pickle_data/takmicenjemozzabin")

if __name__ == "__main__":
    run()
