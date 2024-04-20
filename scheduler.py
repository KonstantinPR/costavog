import schedule
import logging
import time
from worker import run_batched_get_rating


def print_message():
    logging.info("Scheduler is working")


if __name__ == '__main__':
    # Schedule a job to print message every 1 hour
    schedule.every().hour.do(print_message)
    # Schedule a job to run batched_get_rating every 1 hour
    schedule.every().hour.do(run_batched_get_rating)

    # Run the scheduler continuously
    while True:
        schedule.run_pending()
        time.sleep(1)
