from threading import Lock
import datetime
import time
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import urllib3
from io import StringIO
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from io import StringIO
import re
import warnings
import ast
import sqlite3
import yfinance as yf
import json
import sys
import io

from utils.base_processor import BaseProcessor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=FutureWarning, module='pandas')

class CorporateEventsProcessor(BaseProcessor):
    '''
    docstrings
    '''
    def __init__(self):
        '''
        docstrings
        '''
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        # # Initialize the WebDriver
        # self.driver, self.driver_wait = self._initialize_driver()

    def process_instance(self, sub_batch, progress):
        """
        Process a single batch by delegating 
        from abstract base_processor method 
        to this class process_batch (true process info method) 
        via this process_instance method (create instance methos).
        
        sub_batch
        progress

        return result from process_batch
        """
        try:
            ticker_list = sub_batch['ticker_code']
            extra_info = [f"Worker {progress['thread_id']}", ' '.join(ticker_list)]
            self.print_info(progress['batch_index'], progress['total_batches'], progress['start_time'], extra_info)

            # Delegate to process_batch for the actual batch processing
            result = self.process_batch(sub_batch, progress)

        except Exception as e:
            pass

        return result

    def process_batch(self, sub_batch, progress):
        '''
        '''
        result = pd.DataFrame()
        try:
            data = []
            start_time = time.time()
            for i, row in sub_batch.reset_index().iterrows():
                start_date = row['date']
                company_name = row['company_name']
                ticker = row['ticker']
                ticker_code = row['ticker_code']

                try:
                    # Redirect stderr to silence error messages
                    old_stderr = sys.stderr
                    sys.stderr = io.StringIO()

                    ticker_obj = yf.Ticker(ticker_code + '.SA')
                    historical_data = ticker_obj.history(start=start_date, actions=True, auto_adjust=True).reset_index()

                    # historical_data = yf.download(ticker_code + '.SA', start=start_date, progress=False, actions=True, auto_adjust=True).reset_index()
                    if not historical_data.empty:
                        historical_data.columns = historical_data.columns.str.lower().str.replace(" ", "_")
                        historical_data['date'] = pd.to_datetime(historical_data['date']).dt.strftime('%Y-%m-%d')
                        historical_data['company_name'] = company_name
                        historical_data['ticker'] = ticker
                        historical_data['ticker_code'] = ticker_code
                        historical_data = historical_data[self.config.historical_stock_data_all_columns]
                    else:
                        new_row = {
                            'company_name': company_name,
                            'ticker': ticker,
                            'ticker_code': ticker_code,
                        }
                        historical_data = pd.DataFrame([new_row])

                finally:
                    # Reset stderr to its original state
                    sys.stderr = old_stderr

                data.append(historical_data)

                extra_info = [f'Worker {progress["batch_index"]}', ticker_code, company_name]
                self.print_info(i, len(sub_batch), start_time, extra_info, indent_level=2)

            result = pd.concat(data)

        except Exception as e:
            self.log_error(e)

        self.save_to_db(dataframe=result, table_name=self.config.historical_stock_data_table, db_filepath=self.config.metadados_filepath)

        return result

    def get_scrape_targets(self, company_info, historical_data, company_statements):
        '''
        docstrings
        '''
        historical_data_primary_key_columns = ['company_name', 'ticker', 'ticker_code']
        merging_columns = historical_data_primary_key_columns + ['date']

        def process_ticker_isin(row):
            ticker_codes = json.loads(row['ticker_codes']) if row['ticker_codes'] else []
            isin_codes = json.loads(row['isin_codes']) if row['isin_codes'] else []
            ticker_isin = [pair for pair in sorted(list(zip(ticker_codes, isin_codes)), key=lambda x: x[0]) if 'ACN' in pair[1]]
            return ticker_isin

        try:
            # prepare company_info
            company_info['ticker_isin'] = company_info.apply(process_ticker_isin, axis=1)
            mask = company_info['ticker_isin'].apply(lambda x: len(x) > 0)
            company_info = company_info[mask]

            company_info = company_info.explode('ticker_isin')
            company_info[['ticker_code', 'isin_code']] = pd.DataFrame(company_info['ticker_isin'].tolist(), index=company_info.index)
            company_info = company_info.drop(columns=['ticker_isin'])

            # prepare historical_data
            try:
                historical_data['date'] = pd.to_datetime(historical_data['date'])
                # Sort the DataFrame by date in descending order (most recent first)
                historical_data = historical_data.sort_values(by='date', ascending=False)
                # Drop duplicates based on ['company_name', 'ticker', 'ticker_code'], keeping the most recent
                historical_data = historical_data.drop_duplicates(subset=historical_data_primary_key_columns, keep='first')
            except Exception as e:
                company_info['date'] = self.config.stock_data_start_date
                scrape_targets = company_info[merging_columns]
                return scrape_targets

            # get unprocessed companies
            # Merge to find unprocessed companies (companies in `company_info` not in `historical_data`)
            unprocessed_companies = pd.merge(
                company_info,
                historical_data[historical_data_primary_key_columns],
                on=historical_data_primary_key_columns,
                how='left',
                indicator=True
            ).query('_merge == "left_only"').drop(columns=['_merge'])
            unprocessed_companies['date'] = pd.to_datetime('1960-01-01').strftime('%Y-%m-%d')
            unprocessed_companies = unprocessed_companies[merging_columns]

            # Filter rows based on the date threshold
            non_existing_historical_data = historical_data[historical_data['date'].isna()][merging_columns]

            delta = datetime.timedelta(days=self.config.update_days + 1)
            date_diff = datetime.datetime.now() - delta
            processed_companies = historical_data[historical_data['date'] < (date_diff)]
            processed_companies.loc[:, 'date'] = processed_companies['date'].dt.strftime('%Y-%m-%d')
            processed_companies = processed_companies[merging_columns]

            # Combine the datasets
            if not unprocessed_companies.empty:
                scrape_targets = pd.concat([unprocessed_companies, processed_companies], ignore_index=True)
                scrape_targets['date'] = pd.to_datetime(scrape_targets['date'], errors='coerce')
                scrape_targets['date'] = scrape_targets['date'].dt.strftime('%Y-%m-%d')
                scrape_targets = scrape_targets.dropna(subset=['date']).reset_index(drop=True)
            else:
                scrape_targets = processed_companies

        except Exception as e:
            self.log_error(e)
            scrape_targets = pd.DataFrame(columns=merging_columns)

        return scrape_targets

    def main(self, thread=True):
        '''
        docstring
        '''
        try:
            # Load existing information
            company_info = self.load_data(table_name=self.config.company_table, db_filepath=self.config.metadados_filepath)
            historical_data = self.load_data(table_name=self.config.historical_stock_data_table, db_filepath=self.config.metadados_filepath)
            # company_statements = self.load_data(table_name=self.config.initial_table, db_filepath=self.config.initial_filepath)
            company_statements = pd.DataFrame(columns=self.config.statements_columns)

            scrape_targets = self.get_scrape_targets(company_info, historical_data, company_statements)

            # if no scrape_targets, optimize db and return True
            if scrape_targets.size == 0:  # Check if the array is empty
                self.db_optimize(self.config.metadados_filepath)
                return True

            # Process targets using threading or sequential logic
            processed_batch = self.run(scrape_targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__)

            # save/update db
            if not processed_batch.empty:
                self.save_to_db(dataframe=processed_batch, table_name=self.config.historical_stock_data_table, db_filepath=self.config.metadados_filepath)

        except Exception as e:
            self.log_error(e)

        return True
