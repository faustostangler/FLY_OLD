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
        via this process_instance method (create instance method).
        
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

        try:
            # prepare company_info
            company_info = self.explode_company(company_info)

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

class EventsStatementsProcessor(BaseProcessor):
    '''
    definitions
    '''
    def __init__(self):
        '''
        definitions
        '''
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

    def get_scrape_targets(self, company_info):
        '''
        definitions
        '''
        try:
            scrape_targets = self.explode_company(company_info)

        except Exception as e:
            self.log_error(e)

        return scrape_targets

    def get_company_statements(self, company_name):
        '''
        definitions
        '''
        index_columns = self.config.statements_index_columns
        pivot_columns = self.config.statements_pivot_columns

        try:
            sql_company_statements = '''SELECT * 
                        FROM statements_standart 
                        WHERE company_name = ?'''
            company_statements = self.load_data(table_name=self.config.standart_table, query=sql_company_statements, params=(company_name,), db_filepath=self.config.standart_filepath, alert=False)

            # Convert 'quarter' to datetime for correct sorting and filtering (if not already)
            company_statements['quarter'] = pd.to_datetime(company_statements['quarter'])

            # Find the maximum version for each quarter
            latest_versions = company_statements.groupby('quarter')['version'].max().reset_index()

            # Merge with the original DataFrame to keep only rows with the latest version
            company_statements = company_statements.merge(latest_versions, on=['quarter', 'version'])

            # only stock to match splits
            stock_data = company_statements[company_statements['account'].str.startswith(self.config.stock_start)]

            ### Dados da Empresa

            statements = stock_data.pivot_table(
                index=index_columns,  # Use main_columns as the index
                columns=pivot_columns,  # Pivot on 'account'
                values='value',  # Use 'value' as data
                aggfunc='first'  # Use 'first' to handle duplicates
            ).reset_index()

            # Flatten columns if needed
            statements.columns = [f"{col[0]}{self.config.joint}{col[1]}{self.config.joint}{col[2]}{self.config.joint}{col[3]}" if isinstance(col, tuple) and col[0].startswith(self.config.stock_start) else col[0] for col in statements.columns]


        except Exception as e:
            statements = pd.DataFrame()
            self.log_error(e)

        return statements

    def get_company_stock_data(self, ticker_code):
        '''
        definitions
        '''
        try:
            sql_stock_data = '''SELECT *
                        FROM stock_data
                        WHERE ticker_code = ?'''

            company_stock_data = self.load_data(table_name=self.config.historical_stock_data_table, query=sql_stock_data, params=(ticker_code,), db_filepath=self.config.metadados_filepath, alert=False)
            company_stock_data = company_stock_data.dropna(subset=['date'])

            company_stock_data['date'] = pd.to_datetime(company_stock_data['date'])

        except Exception as e:
            company_stock_data = pd.DataFrame()
            self.log_error(e)

        return company_stock_data

    def parse_statements_daily(self, statements, stock_data):
        '''
        definitions
        '''
        try:
            # Align the daily dates with the stock data dates
            daily_dates = stock_data[['date']].drop_duplicates()

            # Expand statements to include daily rows matching stock_data dates
            statements_daily = pd.merge(daily_dates, statements, left_on='date', right_on='quarter', how='left')

            statements_daily["date"] = pd.to_datetime(statements_daily["date"], errors='coerce')
            statements_daily["quarter"] = pd.to_datetime(statements_daily["quarter"], errors='coerce')

            # Sort by date to ensure proper backfilling
            statements_daily = statements_daily.sort_values(by="date")

        except Exception as e:
            statements_daily = pd.DataFrame()
            self.log_error(e)

        return statements_daily

    def parse_statements_filled(self, statements, statements_daily, splits):
        '''
        definitions
        '''
        try:
            columns_to_update = [col for col in statements.columns if col.startswith(self.config.stock_start)]
            
            # Create a copy to avoid modifying the original DataFrame
            statements_daily_filled = statements_daily.copy()

            # Generate quarter-matching column for each date
            statements_daily_filled["date_quarter"] = statements_daily_filled["date"].dt.to_period("Q").dt.end_time.dt.normalize()

            # Get unique quarters
            unique_quarters = statements_daily["quarter"].unique()

            # List to store processed data
            filled_list = {}
            last_quarter_values = None  # Store the last known values from the previous quarter iteration

            # Loop through each quarter and apply backfill
            for quarter in unique_quarters:
                if pd.notna(quarter):  # Ensure the quarter is valid (not NaT)
                    quarter = pd.Timestamp(quarter).normalize()  # Convert quarter to exact midnight timestamp

                    # Select rows belonging to the quarter
                    mask = statements_daily_filled["date_quarter"] == quarter
                    quarter_data = statements_daily_filled.loc[mask].copy()  # Copy to avoid modifying original DF

                    # Apply backfill within the quarter
                    quarter_data = quarter_data.bfill()

                    # Check if there is a stock split within the quarter
                    split_mask = (splits["date"] >= quarter_data["date"].min()) & (splits["date"] <= quarter_data["date"].max())
                    quarter_splits = splits.loc[split_mask]

                    # If there are splits, adjust values accordingly
                    for _, split_row in quarter_splits.iterrows():
                        split_date = split_row["date"]
                        split_factor = split_row["stock_splits"]

                        # Identify rows BEFORE the split date → Divide by split factor
                        before_split_mask = quarter_data["date"] < split_date

                        # Try to apply last known values from previous quarter
                        if last_quarter_values is not None:
                            quarter_data.loc[before_split_mask, columns_to_update] = last_quarter_values.values
                        else:
                            # If last known values are unavailable, apply division as a failsafe
                            quarter_data.loc[before_split_mask, columns_to_update] = (quarter_data.loc[before_split_mask, columns_to_update] / split_factor).round(2).astype(int)

                        # # Identify rows ON and AFTER the split date → Multiply by split factor
                        # after_split_mask = quarter_data["date"] >= split_date
                        # quarter_data.loc[after_split_mask, columns_to_update] = (quarter_data.loc[after_split_mask, columns_to_update] * split_factor).round(2).astype(int)

                    # Store the last known values for the next quarter
                    last_quarter_values = quarter_data[columns_to_update].iloc[-1]
                    
                    quarter_data = quarter_data.drop(columns=["date_quarter"], errors="ignore")
                    filled_list[quarter] = quarter_data  # Append processed quarter data

            # Concatenate all filled data back together
            result = pd.concat(filled_list).sort_values(by="date").reset_index(drop=True)

        except Exception as e:
            self.log_error(e)

        return result

    def method(self, parameter):
        '''
        definitions
        '''
        try:
            pass

        except Exception as e:
            self.log_error(e)

        return parameter

    def main(self, thread=True):
        '''
        definitions
        '''
        index_columns = self.config.statements_index_columns
        pivot_columns = self.config.statements_pivot_columns

        try:
            # load existing company_info data
            company_info = self.load_data(table_name=self.config.company_table, db_filepath=self.config.metadados_filepath)

            scrape_targets = self.get_scrape_targets(company_info)

            # loop companies, load stock_market data and statements_data per company
            start_time = time.time()
            for i, row in scrape_targets.iterrows():
                ticker = row['ticker']
                ticker_code = row['ticker_code']
                company_name = row['company_name']

                if ticker != 'ALPA':
                    continue

                statements_quaterly = self.get_company_statements(company_name)

                stock_data = self.get_company_stock_data(ticker_code)

                if not stock_data.empty:
                    splits = stock_data[['company_name', 'ticker', 'ticker_code', 'date', 'stock_splits']].query("stock_splits != 0")
                    splits['date'] = pd.to_datetime(splits['date'])

                # Ensure date columns are in datetime format

                statements_daily = self.parse_statements_daily(statements_quaterly, stock_data)

                statements_daily = self.parse_statements_filled(statements_quaterly, statements_daily, splits)

                    ### DFs Individuais and DFs Consolidadas
                # Process other groups: 'DFs Individuais' and 'DFs Consolidadas'
                for group_type in ['DFs Individuais', 'DFs Consolidadas']:
                    group_statements = statements_quaterly[statements_quaterly['type'] == group_type]

                    group_statements_pivot = group_statements.pivot_table(
                        index=index_columns, 
                        columns=pivot_columns,
                        values='value',
                        aggfunc='first'
                    ).reset_index()

                    group_statements_pivot.columns = [self.config.joint.join([str(part) for part in col if part]) if isinstance(col, tuple) else col for col in group_statements_pivot.columns]
                    pass

                #     group_statements.to_csv('group_statements.csv')
                #     group_statements_pivot.to_csv('group_statements_pivot.csv')

                #     # Ensure date columns are in consistent datetime format
                #     group_statements_pivot['quarter'] = pd.to_datetime(group_statements_pivot['quarter'])

                #     # Merge group_statements_pivot with daily dates
                #     group_statements_daily = pd.merge(
                #         daily_dates,
                #         group_statements_pivot,
                #         left_on='date',
                #         right_on='quarter',
                #         how='left'
                #     )

                #     # Apply forward-fill and back-fill logic to propagate missing values
                #     group_statements_daily = group_statements_daily.bfill().ffill()






                #     # now merge everything together

                #     # Ensure date columns are in consistent datetime format
                #     statements_daily['date'] = pd.to_datetime(statements_daily['date'])
                #     group_statements_pivot['quarter'] = pd.to_datetime(group_statements_pivot['quarter'])

                #     # Merge group_statements_pivot with statements_daily
                #     merged_statements = pd.merge(
                #         statements_daily,
                #         group_statements_pivot,
                #         left_on='date',
                #         right_on='quarter',
                #         how='outer'
                #     )

                #     # Fill missing values if necessary
                #     merged_statements = merged_statements.bfill().ffill()

                #     # Save the merged result
                #     merged_statements.to_csv(f'{ticker_code}_merged_statements_daily.csv', index=False)


                extra_info = [i, company_name, ticker_code]
                self.print_info(i, len(scrape_targets), start_time, extra_info)





        except Exception as e:
            self.log_error(e)

        return True