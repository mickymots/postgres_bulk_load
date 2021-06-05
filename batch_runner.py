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
   
    run_batch_fn = partial(data_loader, data_dir)
    try :

        print('Threading starts')
        with ThreadPoolExecutor(max_workers=8) as executor:
            try:

                executor.map(run_batch_fn, files)
                executor.shutdown(wait=True)
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)


def main():
    data_dir = '/root/people/data'
    execute_batch(data_dir)
  


if __name__ == '__main__':
    main()