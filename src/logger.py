import logging
import os
from datetime import datetime

log_folder = f"{datetime.now().strftime('%d_%m_%Y')}.log"
log_file = f"{datetime.now().strftime('%d_%m_%Y_%H_%M_%S')}.log"
logs_path = os.path.join(os.getcwd(),'logs',log_folder)
os.makedirs(logs_path,exist_ok=True)

log_file_path = os.path.join(logs_path,log_file)

logging.basicConfig(
    filename=log_file_path,
    format='[%(asctime)s] %(lineno)d %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)