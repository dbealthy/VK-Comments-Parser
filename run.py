import logging
import sys
import os
from subprocess import Popen

import psutil
LOGS_DIRECTORY = 'logs'
if not os.path.exists(LOGS_DIRECTORY):
    os.makedirs(LOGS_DIRECTORY)
logging.basicConfig(filename='logs/run.log', filemode='a', level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


for process in psutil.process_iter():
    if process.cmdline() == ['python', 'main.py']:
        logging.info('Still running: Exit')
        sys.exit()

logging.info('Process not found: Starting it')
Popen(['python', 'main.py'])

