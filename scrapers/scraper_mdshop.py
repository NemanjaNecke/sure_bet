from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import time
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchFrameException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.remote.webelement import WebElement
import pickle
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import logging
import os


def run():
    # **Configure Logging Inside the Run Function**
    log_dir = 'log'
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(log_dir, 'scrapermdshop.log'),
        filemode='w',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logging.info("Starting scraper_mdshop")
    web = 'https://www.mdshop.ba/sport-prematch?sport=Football'
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

    driver.get(web)

    time.sleep(1.1)
    try:
        cookies = driver.find_element(By.CSS_SELECTOR, '.btn.btn-rounded.btn-sm.btn-outline-primary')
        if cookies:
            cookies.click()
    except NoSuchElementException:
        pass
    # Da li postoji iframe pop up

    def parse_mdshop_date(date_str):
        """
        Parse a date string like "PONEDELJAK, 23. 12. 2024. 04:30"
        into a Python datetime object.
        """
        try:
            # Example input: "PONEDELJAK, 23. 12. 2024. 04:30"
            # Remove the day of the week (PONEDELJAK, etc.)
            date_time_part = date_str.split(",")[1].strip()  # "23. 12. 2024. 04:30"
            # Try parsing with the trailing dot after the year
            try:
                return datetime.strptime(date_time_part, "%d. %m. %Y. %H:%M")
            except ValueError:
                # If parsing fails, try without the trailing dot
                return datetime.strptime(date_time_part, "%d. %m. %Y %H:%M")
        except Exception as e:
            logging.error(f"Error parsing date '{date_str}': {e}")
            return None  # Return None if parsing fails
    
    def check_exists_frame():
        try:
            driver.switch_to.frame("helpcrunch-iframe")
        except NoSuchFrameException:
            return False

        try:
            x_button = driver.find_element(By.ID, 'helpcrunch-popup-close-button')
            x_button.click()        
        except NoSuchElementException:
            return False
        return True


    check_exists_frame()

    date = []

    league = []

    match_time = []
    match_home = []
    match_away = []

    odds_value_1 = []
    odds_value_x = []
    odds_value_2 = []
    less = []
    more = []
    no_goals = []
    goal_goal = []

    time.sleep(1.1)

    driver.switch_to.default_content()
    driver.switch_to.frame('sportIframe')

    def pull_odds():

        page = driver.find_elements(
            By.CSS_SELECTOR, "div.selected-league")

        for i in page:
            datetemp = i.find_element(
                By.CSS_SELECTOR, '.row.bet-info-wrap>div.col.col1>span')
            matchesandodds = i.find_elements(
                By.TAG_NAME, 'app-event')

            for j in matchesandodds:

                timetemp = j.find_element(By.CSS_SELECTOR, 'span.time')
                leaguetemp = j.find_element(
                    By.CSS_SELECTOR, '.region-flag-wrap>div.small-text')
                hometemp = j.find_element(By.CSS_SELECTOR, 'span.home')
                awaytemp = j.find_element(By.CSS_SELECTOR, 'span.away')
                try:
                    odds1_temp = j.find_element(
                        By.CSS_SELECTOR, 'span[data-market="1"]')
                    odds2_temp = j.find_element(
                        By.CSS_SELECTOR, 'span[data-market="2"]')
                    oddsX_temp = j.find_element(
                        By.CSS_SELECTOR, 'span[data-market="X"]')
                except NoSuchElementException:
                    pass
                except StaleElementReferenceException:
                    pass
                try:
                    lesstemp = j.find_element(
                        By.CSS_SELECTOR, 'span[data-market="Manje"]')
                    moretemp = j.find_element(
                        By.CSS_SELECTOR, 'span[data-market="Vise"]')
                    gg_temp = j.find_element(By.CSS_SELECTOR, 'span[data-market="GG"]')
                    ng_temp = j.find_element(By.CSS_SELECTOR, 'span[data-market="NG"]')
                except NoSuchElementException:
                    pass
                match_home.append(hometemp.text)
                match_away.append(awaytemp.text)
                league.append(leaguetemp.text)

                full_date_time_str = datetemp.text + ' ' + timetemp.text
                parsed_datetime = parse_mdshop_date(full_date_time_str)
                if parsed_datetime:
                    date.append(parsed_datetime)  # Append the parsed datetime object
                else:
                    date.append("")  # Handle parsing failure gracefully
                try:
                    odds_value_1.append(odds1_temp.text)
                    odds_value_2.append(odds2_temp.text)
                    odds_value_x.append(oddsX_temp.text)
                    less.append(lesstemp.text)
                    more.append(moretemp.text)
                    no_goals.append(ng_temp.text)
                    goal_goal.append(gg_temp.text)
                except UnboundLocalError:
                    odds_value_1.append('')
                    odds_value_2.append('')
                    odds_value_x.append('')
                    less.append('')
                    more.append('')
                    no_goals.append('')
                    goal_goal.append('')

               

    #time.strptime('00:00', '%H:%M')

    # def date_filter(datetemp:WebElement, timetemp:WebElement):
    #     if time.strptime(timetemp.text, '%H:%M') == time.strptime('00:00', '%H:%M'):
    #         date.append(datetemp.text + ' ' + timetemp.text)
    #     elif len(datetemp) > 1 and time.strptime(timetemp.text, '%H:%M') <= time.strptime('23:59', '%H:%M'):
    #         date.append(datetemp[0].text + ' ' + timetemp.text)
    #     else:
    #         date.append(datetemp.text + ' ' + timetemp.text)


    def go_through_pages():
        try:
            # Get the total number of pages on the first load
            pages = driver.find_element(By.CSS_SELECTOR, 'ul.pagination>li:last-of-type')
            original_page_count = int(pages.text)  # Store the initial page count
            logging.info(f"Page count {original_page_count}")
        except (NoSuchElementException, ValueError):
            logging.error("Pagination element not found or invalid. Stopping.")
            return  # Exit if pagination is not found or page count is invalid

        # Start by pulling odds from the first page
        pull_odds()

        # Loop through pages, but stop when reaching the stored page count
        for i in range(1, original_page_count):  # Start from 1 since the first page is already processed
            time.sleep(0.4)  # Allow some time before interacting with the next page

            try:
                # Find all the pagination buttons
                buttons = driver.find_elements(By.CSS_SELECTOR, 'ul.pagination>li')

                # Iterate over the buttons to find the 'Sledeća' (Next) button
                for button in buttons:
                    # Wait for the 'Sledeća' button to be clickable
                    button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//li[text()='Sledeća']")))
                    if 'disabled' in button.get_attribute('class') or button.get_attribute('aria-disabled') == 'true':
                        logging.info("Sledeća button is disabled. Stopping pagination.")

                        break  # Stop pagination if the button is disabled
                    
                    if button.text == 'Sledeća':  # Click the 'Next' button if found
                        if i >= original_page_count - 1:
                            logging.info("Reached the last page, stopping.")
                            break
                        try:
                            driver.execute_script("arguments[0].click();", button)  # Click using JS to avoid interaction issues
                            time.sleep(0.4)  # Allow time for the next page to load
                            pull_odds()  # Pull odds from the next page
                        except StaleElementReferenceException:
                            logging.info("Stale reference, retrying click.")
                            driver.execute_script("arguments[0].click();", button)
                            time.sleep(0.8)  # Allow time for the next page to load
                            pull_odds()  # Retry pulling odds

            except NoSuchElementException:
                logging.error("Next button not found, stopping pagination.")
                break  # Exit the loop if the 'Next' button is not found
            
            # Stop when the current page index reaches the original page count

    go_through_pages()


    # print(
    #     'league:' +str(len(league))  +'\n'+
    #     'time:' +str(len(match_time)) +'\n'+
    #     'home:' +str(len(match_home)) +'\n'+
    #     'away:' +str(len(match_away)) +'\n'+
    #     '1:' +str(len(odds_value_1)) +'\n'+
    #     'x:' +str(len(odds_value_x)) +'\n'+
    #     '2:' +str(len(odds_value_2)) +'\n'+
    #     'Manje:' +str(len(less)) +'\n'+
    #     'Vise:' +str(len(more)) +'\n'+
    #     'NG:' +str(len(no_goals)) +'\n'+
    #     'GG:' +str(len(goal_goal)) +'\n'
    # )

    xy = {
        'leagues': league,
        'time': date,
        'home': match_home,
        'away': match_away,
        '1': odds_value_1,
        'x': odds_value_x,
        '2': odds_value_2,
        'Manje od 2.5': less,
        'Vise od 2.5': more,
        'NG': no_goals,
        'GG': goal_goal
    }
    array_length = len(set(map(len, xy.values())))
    if array_length > 1:
        # Assign default values to arrays that don't have the same length
        max_length = max(map(len, xy.values()))
        for key, value in xy.items():
            if isinstance(value, str):
                xy[key] = list(value)
            if len(value) < max_length:
                xy[key].extend([''] * (max_length - len(value)))

    df = pd.DataFrame.from_dict(xy)
    # print(df)

    # driver.quit()
    df.to_excel('data/takmicenjemdshop.xlsx')
    output = open('pickle_data/takmicenjeadmiralbin.pkl', 'wb')
    pickle.dump(df, output)
    output.close()

run()