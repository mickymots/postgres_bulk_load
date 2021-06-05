# import csv
# import pandas as pd
from os import getenv
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from datetime import datetime, timedelta
import os

import logging
from os import listdir
from os.path import isfile, join
from loader import data_loader
# import asyncio

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



# execute the batch of tickers for given numbers of days
def execute_batch(data_dir):
        
    files = [f for f in listdir(data_dir) if isfile(join(data_dir, f))]

    run_batch_fn = partial(data_loader)

    print('Threading starts')
    with ThreadPoolExecutor(max_workers=8) as executor:
        
        executor.map(run_batch_fn, files)
        executor.shutdown(wait=True)


def main():
    
    execute_batch('./data')
  


if __name__ == '__main__':
    main()