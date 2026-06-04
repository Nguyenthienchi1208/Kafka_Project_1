import logging 
import os
from logging.handlers import RotatingFileHandler

os.makedirs("logger", exist_ok=True)

logger = logging.getLogger("kafka_pipeline")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")  

#Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

#File handler with rotation
file_handler = RotatingFileHandler("logger/pipeline.log", maxBytes=5*1024*1024, backupCount=3)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
