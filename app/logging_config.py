import logging


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,  # Set the logging level to INFO
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'  # Define the log message format
    )