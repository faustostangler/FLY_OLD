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

from utils.base_processor import BaseProcessor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CorporateEventsProcessor(BaseProcessor):
    '''
    Financial Statements
    '''
    def __init__(self):
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        # Initialize the WebDriver
        self.driver, self.driver_wait = self._initialize_driver()

    def process_instance(self, sub_batch, progress):
        """
        Process a single batch by delegating to process_batch.
        """
        try:

            # print(f'Starting batch {progress["batch_index"]}/{progress["total_batches"]} {100*progress["batch_index"]/progress["total_batches"]:.02f}%')
            batch_processor = CorporateEventsProcessor()
            extra_info = [f"Worker {progress['thread_id']}", ' '.join(sub_batch)]
            self.print_info(progress['batch_index'], progress['total_batches'], progress['start_time'], extra_info)

            result = batch_processor.process_batch(sub_batch, progress)

            batch_processor.close_driver()
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
                ticker = row['ticker']
                cvm_code = row['cvm_code']

                data = self.corporate_events_scraper(ticker, cvm_code)
                data = self.parse_data(data, ticker, cvm_code)
                rows.extend([data])

                extra_info = [ticker, cvm_code, f"{len(data)} items"]
                self.print_info(i, len(sub_batch), start_time, extra_info)

        except Exception as e:
            pass

        filtered_rows = [row for row in rows if not row.empty and not row.isna().all(axis=None)]
        corporate_events = pd.concat(filtered_rows, ignore_index=True) if filtered_rows else pd.DataFrame()

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

    def corporate_events_scraper(self, ticker, cvm_code):
        '''
        Specific data manipulation
        '''
        try:
            # Construct the base URL for fetching ticker-specific historical data
            url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/main/{cvm_code}/{ticker}/corporate-actions'

            self.test_internet()
            self.driver.get(url)

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
            self.log_error(e)

        return data

    def get_scrape_targets(self, company_info, corporate_events):
        '''
        '''
        try:
            # Identify unique cvm_code values from corporate_events
            existing_cvm_codes = corporate_events['cvm_code'].unique()

            # Filter company_info to exclude these cvm_codes
            scrape_targets = company_info[~company_info['cvm_code'].isin(existing_cvm_codes)][['cvm_code', 'ticker']].drop_duplicates()

        except Exception as e:
            scrape_targets = company_info[['cvm_code', 'ticker']].drop_duplicates()

        return scrape_targets

    def main(self, thread=True):
        """
        The main method to scrape historical stock data, parse it, and save it to the database.
        """
        try:
            # Load necessary data
            company_info = self.load_data(table_name=self.config.company_table, db_filepath=self.config.metadados_filepath)
            corporate_events = self.load_data(table_name=self.config.corporate_events_data_table, db_filepath=self.config.metadados_filepath)

            scrape_targets = self.get_scrape_targets(company_info, corporate_events)

            # Exit if no scrape_targets
            if scrape_targets.size == 0:  # Check if the array is empty
                self.db_optimize(self.config.metadados_filepath)
                return True

            # Process targets using threading or sequential logic
            thread=False
            processed_batch = self.run(scrape_targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__)

            if not processed_batch.empty:
                self.save_to_db(dataframe=processed_batch, table_name=self.config.corporate_events_data_table, db_filepath=self.config.metadados_filepath)

        except Exception as e:
            self.log_error(e)

        return True
