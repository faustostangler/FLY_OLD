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
            extra_info = [f"Worker {progress['thread_id']}", ' '.join(sub_batch)]
            self.print_info(progress['batch_index'], progress['total_batches'], progress['start_time'], extra_info)
            # Delegate to process_batch for the actual batch processing
            result = self.process_batch(sub_batch, progress)

        except Exception as e:
            pass

        return result

    def process_batch(self, sub_batch, progress):
        '''
        '''
        result = ''

        try:
            pass
        except Exception as e:
            self.log_error(e)

        return result

    def main(self, thread=True):
        '''
        docstring
        '''
        try:
            # Load existing information
            company_info = self.load_data(table_name=self.config.company_table, db_filepath=self.config.metadados_filepath)
            company_historical_data = self.load_data(table_name=self.config.historical_stock_data_table, db_filepath=self.config.metadados_filepath)
            # company_statements = self.load_data(table_name=self.config.initial_table, db_filepath=self.config.initial_filepath)



            for i, row in company_info.iterrows():
                company_name = row['company_name']
                ticker = row['ticker']
                ticker_codes = json.loads(row['ticker_codes']) if row['ticker_codes'] else []
                isin_codes = json.loads(row['isin_codes']) if row['isin_codes'] else []
                ticker_isin = [pair for pair in sorted(list(zip(ticker_codes, isin_codes)), key=lambda x: x[0]) if 'ACN' in pair[1]]

                # debug fast if per-company, or follow the app design for all-company processing
                if any('BBAS3' == pair[0] for pair in ticker_isin):
                    print('BBAS3 debug')
                    print(i, company_name, ticker, ticker_isin)



            # Fetch Scrape targets
            scrape_targets = pd.DataFrame(company_info)

            # if no scrape_targets, optimize db and return True
            if scrape_targets.size == 0:  # Check if the array is empty
                self.db_optimize(self.config.metadados_filepath)
                return True

            # Process targets using threading or sequential logic
            processed_batch = self.run(scrape_targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__)

            # save/update db
            if not processed_batch.empty:
                self.save_to_db(dataframe=processed_batch, table_name=self.config.historical_tickers_urls_table, db_filepath=self.config.metadados_filepath)

        except Exception as e:
            self.log_error(e)

        return True

class old_CorporateEventsProcessor(BaseProcessor):
    '''
    Financial Statements
    '''
    def __init__(self):
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        # # Initialize the WebDriver
        # self.driver, self.driver_wait = self._initialize_driver()

    def process_instance(self, sub_batch, progress):
        """
        Process a single batch by delegating to process_batch.
        """
        try:

            # print(f'Starting batch {progress["batch_index"]}/{progress["total_batches"]} {100*progress["batch_index"]/progress["total_batches"]:.02f}%')
            batch_processor = CorporateEventsProcessor()
            extra_info = [f"Worker {progress['thread_id']}", ' '.join(sub_batch['ticker_codes'])]
            self.print_info(progress['batch_index'], progress['total_batches'], progress['start_time'], extra_info)

            result = batch_processor.process_batch(sub_batch, progress)

            # batch_processor.close_driver()

            self.save_to_db(dataframe=result, table_name=self.config.corporate_events_data_table, db_filepath=self.config.metadados_filepath)

            return result

        except Exception as e:
            pass

        return result

    def process_batch(self, sub_batch, progress):
        '''
        Batch process skeleton to call specific method
        '''
        rows = []

        try:
            start_time = time.time()  # Track the start time for performance logging
            for i, (_, row) in enumerate(sub_batch.iterrows()):
                ticker_codes = row['ticker_codes']
                cvm_code = row['cvm_code']

                data = self.get_yahoo_events(ticker_codes, cvm_code)
                rows.extend([data])

                if len(data) > 1:
                    size = f"{len(data)} items"
                else:
                    size = ""
                extra_info = [ticker_codes, cvm_code, size]
                self.print_info(i, len(sub_batch), start_time, extra_info, indent_level=1)

        except Exception as e:
            pass

        filtered_rows = [row for row in rows if not row.empty and not row.isna().all(axis=None)]
        if filtered_rows:  # Check if filtered_rows is not empty
            corporate_events = pd.concat(filtered_rows, ignore_index=True)
        else:
            # Provide an empty DataFrame with the same columns as expected in the concatenation
            corporate_events = pd.DataFrame(columns=rows[0].columns if rows else [])

        return corporate_events
            
    def process_batch_b3(self, sub_batch, progress):
        '''
        Batch process skeleton to call specific method
        '''
        rows = []

        try:
            start_time = time.time()  # Track the start time for performance logging
            for i, (_, row) in enumerate(sub_batch.iterrows()):
                ticker = row['ticker']
                cvm_code = row['cvm_code']

                data = self.corporate_events_scraper(ticker, cvm_code)
                data = self.parse_data(data, ticker, cvm_code)
                rows.extend([data])

                if len(data) > 1:
                    size = f"{len(data)} items"
                else:
                    size = ""
                extra_info = [ticker, cvm_code, size]
                self.print_info(i, len(sub_batch), start_time, extra_info, indent_level=1)

        except Exception as e:
            pass

        filtered_rows = [row for row in rows if not row.empty and not row.isna().all(axis=None)]
        if filtered_rows:  # Check if filtered_rows is not empty
            corporate_events = pd.concat(filtered_rows, ignore_index=True)
        else:
            # Provide an empty DataFrame with the same columns as expected in the concatenation
            corporate_events = pd.DataFrame(columns=rows[0].columns if rows else [])

        return corporate_events
    
    def parse_data(self, data, ticker, cvm_code):
        '''
        '''
        columns = self.config.corporate_events_data_columns

        try:
            key = 'subscription'
            try:
                data[key] = data[key][["Subscrição", "Código ISIN", "Negócios com até", "Preço Emissão (R$)"]].rename(
                    columns={
                        "Subscrição": "corporate_action",
                        "Código ISIN": "isin_code",
                        "Negócios com até": "ex_date",
                        "Preço Emissão (R$)": "price_or_factor"
                            })
                data[key]['source'] = key
            except Exception as e:
                data[key] = pd.DataFrame(columns=columns)

            key = "payments_approved"
            try:
                # Match and rename the necessary columns
                data[key] = data[key][["Proventos", "Código ISIN", "Negócios com até", "Valor (R$)"]].rename(
                    columns={
                        "Proventos": "corporate_action",
                        "Código ISIN": "isin_code",
                        "Negócios com até": "ex_date",
                        "Valor (R$)": "price_or_factor"
                    })
                data[key]["source"] = key
            except Exception as e:
                data[key] = pd.DataFrame(columns=columns)

            key = 'assets'
            try:
                data[key] = data[key][["Proventos em Ativos", "Código ISIN", "Negócios com até", "% / Fator de Grupamento"]].rename(
                    columns={
                        "Proventos em Ativos": "corporate_action",
                        "Código ISIN": "isin_code",
                        "Negócios com até": "ex_date",
                        "% / Fator de Grupamento": "price_or_factor"
                        })
                data[key]['source'] = key
            except Exception as e:
                data[key] = pd.DataFrame(columns=columns)

            try:
                key = 'payments'
                data[key]["Valor do Provento (R$) ajustado"] = (
                    pd.to_numeric(data[key]["Valor do Provento (R$)"], errors="coerce") /
                    pd.to_numeric(data[key]["Proventos por unidade ou mil"], errors="coerce"))
                data[key] = data[key][["Tipo do Provento (II)", "Tipo de Ativo", "Últ Dia 'Com'", "Valor do Provento (R$) ajustado"]].rename(
                    columns={
                        "Tipo do Provento (II)": "corporate_action", 
                        "Tipo de Ativo": "isin_code",
                        "Últ Dia 'Com'": "ex_date",
                        "Valor do Provento (R$) ajustado": "price_or_factor",
                        })
                data[key]['source'] = key
            except Exception as e:
                data[key] = pd.DataFrame(columns=columns)

            df_merged = pd.merge(data['subscription'], data['assets'], how="outer")
            merged_df = pd.merge(df_merged, data['payments_approved'], how="outer")
            df_final = pd.merge(data['payments'], merged_df, how="outer")

            if df_final.empty:
                blank_company_df = pd.DataFrame([{'ticker': ticker, 'cvm_code': cvm_code}])
                df_final = pd.concat([df_final,blank_company_df], ignore_index=True)


            df_final['ticker'] = ticker
            df_final['cvm_code'] = cvm_code
            df_final = df_final[columns]
            df_final['ex_date'] = pd.to_datetime(df_final['ex_date'], format='%d/%m/%Y', errors='coerce')
            df_final = df_final[columns].sort_values(by=["ex_date"], ascending=[True])

        except Exception as e:
            df_final = pd.DataFrame(columns=columns)

        return df_final.drop_duplicates().reset_index(drop=True)

    def extract_aprovados(self, button_xpath, xpath, item):
        """
        Extract table data from a web element using its XPath.

        :param xpath: The XPath of the table to extract data from.
        :return: A pandas DataFrame containing the table data.
        """
        try:
            self.click(button_xpath)

            element = self.driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath)))

            html_content = element.get_attribute("outerHTML")
            cleaned_html = str(html_content).replace('.', '').replace(',', '.')
            df = pd.read_html(StringIO(cleaned_html))[0]  # Convert the table to a DataFrame

            self.click(button_xpath)

        except Exception as e:
            df = pd.DataFrame()

        return df

    def extract_dinheiro(self, button_xpath, xpath):
        """
        Extract table data from a web element using its XPath.

        :param xpath: The XPath of the table to extract data from.
        :return: A pandas DataFrame containing the table data.
        """
        try:
            try:
                element = self.driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            except Exception as e:
                self.click(button_xpath)
                element = self.driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath)))

            # get raw pagination
            raw_code = []
            # select_page_xpath = '//*[@id="selectPage"]'
            pagination_xpath = '//*[@id="listing_pagination"]/pagination-template/ul'
            nav_bloc_xpath = '//*[@id="accordionMoney"]/div/table'

            ul_xpath = '//*[@id="listing_pagination"]/pagination-template/ul'
            ul_element = self.driver.find_element(By.XPATH, ul_xpath)
            paginagion_next_item = len(ul_element.find_elements(By.TAG_NAME, "li"))
            next_page_xpath = f'//*[@id="listing_pagination"]/pagination-template/ul/li[{paginagion_next_item}]/a'

            # time.sleep(self.config.wait_time/5)
            text = self.text(pagination_xpath, self.driver_wait)
            pages = list(map(int, re.findall(r'\d+', text)))
            total_pages = max(pages) - 1

            raw_code = []
            start_time = time.time()
            for i, page in enumerate(range(0, total_pages + 1)):
                self.wait_forever(self.driver_wait, nav_bloc_xpath)
                inner_html = self.raw_text(nav_bloc_xpath, self.driver_wait)
                raw_code.append(inner_html)

                if i != total_pages:
                    self.click(next_page_xpath, self.driver_wait)

                extra_info = [f'page {page + 1}']
                # self.print_info(i, total_pages + 1, start_time, extra_info)
                time.sleep(0.05)

            # Combine raw_code into one HTML string
            html_content = ''.join(raw_code)
            cleaned_html = str(html_content).replace('.', '').replace(',', '.')

            # Parse the HTML using BeautifulSoup
            soup = BeautifulSoup(cleaned_html, 'html.parser')

            # Extract the table headers
            header_row = soup.find('tr')
            headers = [th.text.strip() for th in header_row.find_all('th')]

            # Extract the table rows
            rows = []
            for tr in soup.find_all('tr', class_="GridRow_SiteBmfBovespa"):
                cells = [td.text.strip() for td in tr.find_all('td')]
                rows.append(cells)

            # Create a DataFrame
            df = pd.DataFrame(rows, columns=headers)

        except Exception as e:
            print(len(df), df.columns)
            print(df)
            self.log_error(f"Error extracting table data from: {xpath} - {e}")
            df = pd.DataFrame()

        return df

    def get_yahoo_events(self, ticker_codes, cvm_code, start_date='1960-01-01'):
        '''
        '''
        result = ''
        try:
            # # debug
            # ticker_codes = 'BBAS3'

            try:
                # Redirect stderr to silence error messages
                old_stderr = sys.stderr
                sys.stderr = io.StringIO()

                # Attempt to download data
                historical_splits_columns = ['date', 'dividends', 'stock_splits']
                historical_splits = yf.download(ticker_codes + '.SA', start=start_date, progress=False, actions=True, auto_adjust=True).reset_index()
                historical_splits.columns = historical_splits.columns.get_level_values(0).rename(None)
                historical_splits.rename(columns={'Date': 'date', 'Dividends': 'dividends', 'Stock Splits': 'stock_splits'}, inplace=True)
                historical_splits = historical_splits[historical_splits_columns]
                historical_splits = historical_splits.loc[~((historical_splits['dividends'] == 0) & (historical_splits['stock_splits'] == 0))]
                historical_splits.replace(0, np.nan, inplace=True)
                historical_splits['cvm_code'] = cvm_code
                historical_splits['ticker_codes'] = ticker_codes
                historical_splits = historical_splits[['date', 'cvm_code', 'ticker_codes', 'dividends', 'stock_splits']]

            finally:
                # Reset stderr to its original state
                sys.stderr = old_stderr

        except Exception as e:
            # self.log_error(e)
            historical_splits = pd.DataFrame(columns=historical_splits_columns)

        return historical_splits

    def corporate_events_scraper(self, ticker, cvm_code):
        '''
        Specific data manipulation
        '''
        try:
            # Construct the base URL for fetching ticker-specific historical data
            url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/main/{cvm_code}/{ticker}/corporate-actions'

            self.test_internet()
            self.driver.get(url)

            time.sleep(self.config.wait_time)
            data = {}

            xpath = '//*[@id="accordion"]'
            element_text = self.driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath))).text
            if not element_text:
                data['assets'] = pd.DataFrame()
                data['payments_approved'] = pd.DataFrame()
                data['subscription'] = pd.DataFrame()

            else:
                # Proventos Aprovados em Ativos
                key = 'assets'
                button_xpath = '//*[@id="accordionHeading"]/div/div/a'
                table_xpath = '//*[@id="accordionBody"]'
                item = "Ativos"
                if item not in element_text:
                    data[key] = pd.DataFrame()
                else:
                    data[key] = self.extract_aprovados(button_xpath, table_xpath, item)

                # Proventos Aprovados em Dinheiro
                key = 'payments_approved'
                button_xpath = '//*[@id="accordionHeadingTwo"]/div/div/a'
                table_xpath = '//*[@id="accordionBodyTwo"]'
                item = "Dinheiro"
                if item not in element_text:
                    data[key] = pd.DataFrame()
                else:
                    data[key] = self.extract_aprovados(button_xpath, table_xpath, item)

                # Proventos Aprovados em Subscrição
                key = 'subscription'
                button_xpath = '//*[@id="accordionHeadingThree"]/div/div/a'
                table_xpath = '//*[@id="accordionBodyThree"]'
                item = "Subscrição"
                if item not in element_text:
                    data[key] = pd.DataFrame()
                else:
                    data[key] = self.extract_aprovados(button_xpath, table_xpath, item)

            dropdown_xpath = '//*[@id="selectType"]'
            dropdown_value = '2'  # Corresponds to "Proventos em Dinheiro"
            click = self.choose_by_value(dropdown_xpath, dropdown_value)

            # Proventos Pagos em Dinheiro
            key = 'payments'

            check_text = 'Não há dados'
            xpath = '//*[@id="divContainerIframeB3"]/app-companies-corporate-actions/div[3]/div/app-companies-corporate-actions-money/form'
            element_text = self.driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath))).text

            if check_text in element_text:
                data[key] = pd.DataFrame()
            else:
                button_xpath = '//*[@id="accordionHeading"]/div/div/a'
                table_xpath = '//*[@id="accordionMoney"]'
                data[key] = self.extract_dinheiro(button_xpath, table_xpath)

        except Exception as e:
            # self.log_error(e)
            pass
        return data

    def get_scrape_targets(self, company_info, corporate_events):
        """
        Determine the scrape targets by exploding ticker codes and filtering out companies already present in corporate events.

        Args:
            company_info (pd.DataFrame): DataFrame containing company information with ticker codes.
            corporate_events (pd.DataFrame): DataFrame containing corporate events with cvm_code.

        Returns:
            pd.DataFrame: DataFrame containing scrape targets with ticker_codes and company_name.
        """
        try:
            # Parse `ticker_codes` as JSON and explode into separate rows
            company_info = company_info.copy()
            company_info['ticker_codes'] = company_info['ticker_codes'].apply(lambda x: json.loads(x) if isinstance(x, str) else [])
            exploded_company_info = company_info.explode('ticker_codes')[['ticker_codes', 'cvm_code', 'company_name']]
            exploded_company_info = exploded_company_info[exploded_company_info['ticker_codes'].notna() & (exploded_company_info['ticker_codes'] != '')]

            # Identify unique cvm_code values from corporate_events
            existing_cvm_codes = corporate_events['cvm_code'].unique()

            # Filter exploded_company_info to exclude these cvm_codes
            scrape_targets = exploded_company_info[~exploded_company_info['cvm_code'].isin(existing_cvm_codes)]

            # Keep unique rows
            scrape_targets = scrape_targets.drop_duplicates()

        except Exception as e:
            # print(f"Error processing scrape targets: {e}")
            # If an error occurs, fallback to returning all exploded data
            scrape_targets = exploded_company_info

        return scrape_targets

    def main(self, thread=True):
        """
        The main method to scrape historical stock data, parse it, and save it to the database.
        """
        try:
            # Load necessary data987
            company_info = self.load_data(table_name=self.config.company_table, db_filepath=self.config.metadados_filepath)
            corporate_events = self.load_data(table_name=self.config.corporate_events_data_table, db_filepath=self.config.metadados_filepath)

            scrape_targets = self.get_scrape_targets(company_info, corporate_events)

            # Exit if no scrape_targets
            if scrape_targets.size == 0:  # Check if the array is empty
                self.db_optimize(self.config.metadados_filepath)
                return True

            # Process targets using threading or sequential logic
            processed_batch = self.run(scrape_targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__)

            if not processed_batch.empty:
                self.save_to_db(dataframe=processed_batch, table_name=self.config.corporate_events_data_table, db_filepath=self.config.metadados_filepath)

        except Exception as e:
            self.log_error(e)

        return True

class old_CorporateEventsMerger(BaseProcessor):
    '''
    Financial Statements
    '''
    def __init__(self):
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        # # Initialize the WebDriver
        # self.driver, self.driver_wait = self._initialize_driver()

    def old_zip_ticker_isin_codes(self, all_company_info):
        '''
        '''
        try:
            # Ensure lists in `ticker_codes` and `isin_codes` are properly parsed
            all_company_info['ticker_codes'] = all_company_info['ticker_codes'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else [])
            all_company_info['isin_codes'] = all_company_info['isin_codes'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else [])

            # Step 1: Add `ticker_isin` column to `company_info`
            all_company_info['ticker_isin'] = all_company_info.apply(
                lambda row: [(ticker, isin) for ticker, isin in zip(row['ticker_codes'], row['isin_codes']) if 'ACN' in isin],
                axis=1
            )

            return all_company_info
        except Exception as e:
            self.log_error(e)

    def old_load_company_historical_data(self, data, ticker, tick, er, isin):
        '''
        '''
        try:
            # load historical_data['ticker']
            # SQL query
            query_historical_data = f"""
            SELECT *
            FROM {self.config.historical_data}
            WHERE ticker = ?
            """

            # Load corporate_events['isin']
            # SQL query
            query_corporate_events = f"""
            SELECT *
            FROM {self.config.corporate_events_data_table}
            WHERE ticker = ? AND (isin_code = ? OR isin_code = ?)
            """

            # Load data into a Pandas DataFrame
            with sqlite3.connect(self.config.metadados_filepath) as conn:
                data[ticker]['historical_data'] = pd.read_sql_query(query_historical_data, conn, params=(ticker,))
                data[ticker]['corporate_events'] = pd.read_sql_query(query_corporate_events, conn, params=(tick, er, isin,))

            # Convert date columns to datetime for proper alignment
            data[ticker]['historical_data']['date'] = pd.to_datetime(data[ticker]['historical_data']['date'])
            data[ticker]['corporate_events']['ex_date'] = pd.to_datetime(data[ticker]['corporate_events']['ex_date'])

            # sorting
            data[ticker]['historical_data'] = data[ticker]['historical_data'].sort_values(by='date')
            data[ticker]['corporate_events'] = data[ticker]['corporate_events'].sort_values(by='ex_date')

        except Exception as e:
            self.log_error(e)

        return data

    def old_load_company_statements_data(self, data, ticker, company_name):
        '''
        '''
        try:
            db_filepath_statements = self.config.initial_filepath
            db_filepath_statements = r"D:\Fausto Stangler\Documentos\Python\FLY\backend\data\statements initial - Copy.db" # debugging
            print('debugfg filepath')

            # load statements['company_name'] BCO BRASIL SA & description contains ON, da isin do company_info, and types
            types = 'Dados da Empresa'

            query = f"""
            SELECT *
            FROM {self.config.statements_file}
            WHERE company_name = ?
            """

            # Execute the query
            with sqlite3.connect(db_filepath_statements) as conn:
                data[ticker]['statements'] = pd.read_sql_query(query, conn, params=(company_name, ))
            data[ticker]['statements']['quarter'] = pd.to_datetime(data[ticker]['statements']['quarter'])

            data[ticker]['statements'] = data[ticker]['statements'].pivot_table(
                index=['nsd', 'sector', 'subsector', 'segment', 'company_name', 'quarter', 'version'],
                columns=['type', 'frame', 'description', 'account'],  # pivot on type, frame, description, account
                values='value',
                aggfunc='first'  # Use the first value in case of duplicates
            ).reset_index()

            # Flatten the multi-level columns
            data[ticker]['statements'].columns = [
                self.config.joint.join(filter(None, col)).rstrip(self.config.joint)  # Flatten and remove trailing join characters
                if isinstance(col, tuple) else col  # Handle non-multi-level column names
                for col in data[ticker]['statements'].columns
            ]

            # Add new columns with proper naming conventions directly on data[ticker]['statements']
            data[ticker]['statements']['Dados da Empresa - Composição do Capital - Ações ON - 00.03'] = (
                data[ticker]['statements']['Dados da Empresa - Composição do Capital - Ações ON Ordinárias - 00.01.01'].fillna(0) +
                data[ticker]['statements']['Dados da Empresa - Composição do Capital - Em Tesouraria Ações ON Ordinárias - 00.02.01'].fillna(0)
            )

            data[ticker]['statements']['Dados da Empresa - Composição do Capital - Ações PN - 00.04'] = (
                data[ticker]['statements']['Dados da Empresa - Composição do Capital - Ações PN Preferenciais - 00.01.02'].fillna(0) +
                data[ticker]['statements']['Dados da Empresa - Composição do Capital - Em Tesouraria Ações PN Preferenciais - 00.02.02'].fillna(0)
            )


            # Sort the DataFrame by quarter and version in ascending order
            data[ticker]['statements'] = data[ticker]['statements'].sort_values(by=['quarter', 'version'], ascending=[True, False])

            # Drop duplicates, keeping the first occurrence (highest version)
            data[ticker]['statements'] = data[ticker]['statements'].drop_duplicates(subset=['quarter', 'sector', 'subsector', 'segment', 'company_name'], keep='first')

            data[ticker]['statements']['start_quarter_date'] = data[ticker]['statements']['quarter'] - pd.offsets.QuarterBegin(startingMonth=1)

            # Reorder columns
            stock_columns = [col for col in data[ticker]['statements'].columns if self.config.joint in col]
            data_columns = [col for col in data[ticker]['statements'].columns if self.config.joint not in col]
            reordered_columns = sorted(
                stock_columns,
                key=lambda col: (
                    col.split(self.config.joint)[0][::-1] if len(col.split(self.config.joint)) > 0 else '',  # 1st Component (descending)
                    col.split(self.config.joint)[3] if len(col.split(self.config.joint)) > 3 else '',  # 4th Component (ascending)
                    )
                )
            data[ticker]['statements'] = data[ticker]['statements'][data_columns + reordered_columns]

        except Exception as e:
            self.log_error(e)

        return data

    def old_standardize_dataframes(self, data, ticker):
        '''
        Map common columns across dataframes to ensure consistency.
        '''
        try:

            def reorder_columns(df, first_columns):
                """
                Reorder DataFrame columns so that `first_columns` come first if present.

                Args:
                    df (pd.DataFrame): The input DataFrame.
                    first_columns (list): List of column names to prioritize.

                Returns:
                    pd.DataFrame: DataFrame with reordered columns.
                """
                # Determine the columns that should come first and the remaining columns
                first = [col for col in first_columns if col in df.columns]
                others = [col for col in df.columns if col not in first]
                return df[first + others]

            company_info = data[ticker]['company_info']
            historical_data = data[ticker]['historical_data']
            corporate_events = data[ticker]['corporate_events']
            statements_data = data[ticker]['statements']

            # Step 1: Map `isin_code` in `corporate_events` to `ticker` using `ticker_isin`
            ticker_isin_mapping = {}
            for _, row in company_info.iterrows():
                for ticker_code, isin in row['ticker_isin']:
                    ticker_isin_mapping[isin] = ticker_code
            corporate_events['ticker_codes'] = corporate_events['isin_code'].map(ticker_isin_mapping)

            # Step 2: Add `cvm_code` and `ticker` to `statements_data` based on `company_name`
            statements_data = statements_data.merge(
                company_info[['company_name', 'cvm_code', 'ticker']],
                on='company_name',
                how='left'
            )

            # Step 3: Map `historical_data['ticker']` to `company_info['ticker_codes']`
            historical_data = historical_data.merge(
                company_info.explode('ticker_codes')[['ticker_codes', 'cvm_code', 'company_name', 'ticker']],
                left_on=historical_data['ticker'],
                right_on='ticker_codes',
                how='left'
            ).drop(columns=['ticker_x']).rename(columns={'ticker_y': 'ticker'})

            # Step 4: Add `company_name` to `corporate_events` based on `ticker` and `cvm_code`
            corporate_events = corporate_events.merge(
                company_info[['ticker', 'cvm_code', 'company_name']],
                on=['ticker', 'cvm_code'],
                how='left'
            )


            # Step 5: Reorder columns for consistency
            first_columns = ['cvm_code', 'company_name', 'ticker', 'tick', 'ticker_codes']
            company_info = reorder_columns(company_info, first_columns)
            historical_data = reorder_columns(historical_data, first_columns)
            corporate_events = reorder_columns(corporate_events, first_columns)
            statements_data = reorder_columns(statements_data, first_columns)

            # Save updated dataframes back to `data`
            data[ticker]['company_info'] = company_info
            data[ticker]['historical_data'] = historical_data
            data[ticker]['corporate_events'] = corporate_events
            data[ticker]['statements'] = statements_data

        except Exception as e:
            print(f"Error in mutual_mapping: {e}")

        return data

    def old_apply_corporate_events(self, data, ticker):
        '''
        '''
        try:
            company_info = data[ticker]['company_info']
            historical_data = data[ticker]['historical_data']
            corporate_events = data[ticker]['corporate_events']
            statements_data = data[ticker]['statements']

            # filter corporate_events        
            mask = corporate_events['source'].isin(['assets', 'subscription'])
            mask &= corporate_events['ex_date'] >= statements_data['quarter'].min()
            corporate_events_filtered = corporate_events[mask]

            # Perform the asof merge by company_name and align nearest quarter <= date
            historical_statements_data = pd.merge_asof(
                historical_data[['date', 'company_name']],
                statements_data,
                by="company_name",
                left_on="date",
                right_on="quarter",
                direction="backward",   # Get the most recent financial data up to the historical date
            )
            letter_columns = [col for col in historical_statements_data.columns if col[0].isalpha()]
            digit_columns = [col for col in historical_statements_data.columns if col[0].isdigit()]
            historical_statements_data = historical_statements_data[letter_columns + sorted(digit_columns)]


            changes = {}
            for _, event in corporate_events_filtered.iterrows():
                changes[ticker] = {}
                ticker = event['ticker_codes']
                event_date = event['ex_date']
                factor = event['price_or_factor']
                action = event['corporate_action']
                
                # Determine stock type columns based on ticker suffix
                if ticker.endswith("3"):
                    affected_columns = [col for col in historical_statements_data.columns if "ON" in col]
                else:
                    affected_columns = [col for col in historical_statements_data.columns if "PN" in col]

                if action == "DESDOBRAMENTO":
                    calculated_factor = ((100 + factor) / 100) # 100 aumenta de 100 e vira 200, ou seja, multiplica por 2; 400 aumenta de 100 e vira 500, ou seja, multiplica por 5
                elif action == "GRUPAMENTO":
                    calculated_factor = factor # 0,01 divide por 100, ou seja, 100 vira 1
                elif action == "BONIFICACAO":
                    calculated_factor = (100 + factor) / 100 # 30 signifca aumento de 30%, ou seja, 100 vira 130

                changes[ticker]['action'] = action
                changes[ticker]['event_date'] = event_date
                changes[ticker]['factor'] = factor
                changes[ticker]['calculated_factor'] = calculated_factor
                changes[ticker]['affected_columns'] = affected_columns
                
                # Apply factor to rows where the date is greater than the event date
                mask = historical_statements_data['date'] > event_date
                changes[ticker]['com value'] = historical_statements_data.loc[mask, affected_columns].iloc[0]
                historical_statements_data.loc[mask, affected_columns] *= calculated_factor
                changes[ticker]['ex value'] = historical_statements_data.loc[mask, affected_columns].iloc[0]


            data[ticker]['company_info'] = company_info
            data[ticker]['historical_data'] = historical_data
            data[ticker]['corporate_events'] = corporate_events
            data[ticker]['statements_data'] = statements_data
            data[ticker]['historical_statements_data'] = historical_statements_data
            data[ticker]['changes'] = changes

        except Exception as e:
            self.log_error(e)

        return data

    def old_prepare_data(self, data, ticker_isin, company_info_df, company_name):
        '''
        '''
        try:
            for ticker, isin in ticker_isin:
                data[ticker] = {}
                data[ticker]['company_info'] = company_info_df

                tick = ticker[:4]
                er = ticker[4:]
                er = self.config.tipos_acoes[er].split('(')[-1].strip(')')

                data = self.old_load_company_historical_data(data, ticker, tick, er, isin)

                data = self.old_load_company_statements_data(data, ticker, company_name)

                data = self.old_standardize_dataframes(data, ticker)

                data[ticker]['company_info'].to_csv(f'{ticker}_company_info.csv', index=False)
                data[ticker]['historical_data'].to_csv(f'{ticker}_historical_data.csv', index=False)
                data[ticker]['corporate_events'].to_csv(f'{ticker}_corporate_events.csv', index=False)
                data[ticker]['statements'].to_csv(f'{ticker}_statements.csv', index=False)

                data = self.old_apply_corporate_events(data, ticker)

                data[ticker]['company_info'].to_csv(f'{ticker}_company_info.csv', index=False)
                data[ticker]['historical_data'].to_csv(f'{ticker}_historical_data.csv', index=False)
                data[ticker]['corporate_events'].to_csv(f'{ticker}_corporate_events.csv', index=False)
                data[ticker]['statements'].to_csv(f'{ticker}_statements.csv', index=False)

        except Exception as e:
            self.log_error(e)

        return data

    def main(self, thread=True):
        '''
        '''
        try:
            # load company data and prepare it
            # carregar a company_info
            all_company_info = self.load_data(table_name=self.config.company_table, db_filepath=self.config.metadados_filepath)
            
            # Adjust Ticker and Isin Data
            all_company_info = self.old_zip_ticker_isin_codes(all_company_info)

            # Iterate over company in company_df
            data = {}
            start_time = time.time()
            for i, row in all_company_info.iterrows():
                cvm_code = row['cvm_code']
                company_name = row['company_name']
                ticker_isin = row['ticker_isin']
                company_info_df= pd.DataFrame(row).T

                extra_info = [cvm_code, company_name]
                self.print_info(i, len(all_company_info), start_time, extra_info)

                if ticker_isin and company_name == 'BCO BRASIL SA':
                    data = self.old_prepare_data(data, ticker_isin, company_info_df, company_name)

            # save data
            # to implement


        except Exception as e:
            self.log_error(e)

        return True