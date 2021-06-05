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
from df_worker import data_loader


# execute the batch of tickers for given numbers of days
def execute_batch(data_dir):
        
    files = [data_dir + f for f in listdir(data_dir) if isfile(join(data_dir, f))]
    for f in files:
        logging.info('files {f}')
    run_batch_fn = partial(data_loader)
    try :

        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(run_batch_fn, files)
            executor.shutdown(wait=True)
           
    except Exception as e:
        logging.error(e)


def main():
    logging.basicConfig(filename='batch_runner.log', filemode='a', format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', datefmt='%H:%M:%S', level=logging.INFO)
    logging.info('Started')
    
    ts_start = datetime.now()
    data_dir = './data/'
    execute_batch(data_dir)

    logging.info(f'Processing Took %s seconds {datetime.now() - ts_start}')  
    logging.info('Finished')
  


if __name__ == '__main__':
    main()