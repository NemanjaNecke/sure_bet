from api import betole, sportplus, betlive, maxbet
import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

from surebet import find_surebet

# Configuration
LOG_DIR = 'log'
LOG_FILE = os.path.join(LOG_DIR, 'main_process.log')
MAX_CONCURRENT_SCRAPERS = 10  # Adjust based on your system
RETRIES = 3
INITIAL_DELAY = 5  # seconds
def setup_main_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logging.basicConfig(
        filename=LOG_FILE,
        filemode='a',  # Append mode
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    # Console logging has been removed so logs go only to the file.


async def run_with_retries(scraper_func, executor, retries=RETRIES, initial_delay=INITIAL_DELAY):
    """
    Run a single scraper function in a thread with retries + exponential backoff.
    """
    scraper_name = scraper_func.__name__
    attempt = 0
    delay = initial_delay
    while attempt < retries:
        try:
            logging.info(f"Running {scraper_name} (Attempt {attempt + 1})")
            start_time = time.perf_counter()
            await asyncio.get_event_loop().run_in_executor(executor, scraper_func)
            end_time = time.perf_counter()
            logging.info(f"{scraper_name} completed in {end_time - start_time:.2f} seconds.")
            return f"{scraper_name} finished successfully."
        except Exception as e:
            attempt += 1
            logging.error(f"Error in {scraper_name}: {e}")
            if attempt < retries:
                logging.warning(f"Retrying {scraper_name} in {delay} seconds...")
                await asyncio.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logging.error(f"{scraper_name} failed after {retries} attempts.")
                return f"{scraper_name} failed after {retries} attempts."


async def main():
    setup_main_logging()

    # Map each scraper function to the number of times it should be run.
    # All scrapers run once except scraper_xlivebet, which runs 4 times concurrently.
    scraper_tasks = {
        sportplus.run: 1,  # API scraper
        betlive.run: 1,  # API scraper
        betole.run: 1,  # API scraper
        maxbet.run: 1
    }

    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SCRAPERS) as executor:
        # For each scraper function, schedule the desired number of runs.
        for scraper_func, count in scraper_tasks.items():
            for _ in range(count):
                tasks.append(run_with_retries(scraper_func, executor))
        results = await asyncio.gather(*tasks)

    for result in results:
        logging.info(result)

    # Run surebet calculations after scrapers finish.
    try:
        find_surebet.main()
        logging.info("Surebet calculations completed successfully.")
    except Exception as e:
        logging.error(f"Error running surebet: {e}")

    logging.info("All scrapers have finished running.")


if __name__ == "__main__":
    asyncio.run(main())