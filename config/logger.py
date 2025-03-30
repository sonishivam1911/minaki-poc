import logging

# Centralized logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("process_logs.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("centralized_logger")